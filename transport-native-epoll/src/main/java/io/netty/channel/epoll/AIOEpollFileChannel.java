/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.epoll;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.Socket;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.LongObjectMap;


public class AIOEpollFileChannel extends AsynchronousFileChannel {

    private final FileDescriptor file;
    private final FileDescriptor eventFd;
    private final EpollEventLoop epollEventLoop;
    private final EventFileChannel nettyChannel;
    private final LongObjectMap<IORequest> outstandingRequests;

    class IORequest<A> {
        public final ByteBuffer buffer;
        public final CompletionHandler handler;
        public final A attachment;

        IORequest(ByteBuffer buffer, CompletionHandler handler, A attachment) {
            this.buffer = buffer;
            this.handler = handler;
            this.attachment = attachment;
        }
    }

    public AIOEpollFileChannel(File file, EpollEventLoop eventLoop) throws IOException {
        this.file = FileDescriptor.from(file, FileDescriptor.O_RDONLY);
        this.eventFd = Native.newEventFd();
        this.epollEventLoop = eventLoop;
        this.nettyChannel = new EventFileChannel(this);
        this.outstandingRequests = new LongObjectHashMap<IORequest>(128);

        Runnable register = new Runnable() {
            public void run() {
                try {
                    nettyChannel.doRegister();
                } catch (Exception e) {
                    throw new IOError(e);
                }
            }
        };

        if (epollEventLoop.inEventLoop()) {
            register.run();
        } else {
            epollEventLoop.submit(register);
        }
    }

    public long size() throws IOException {
        return file.length();
    }

    public AsynchronousFileChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void force(boolean metaData) throws IOException {
        throw new UnsupportedOperationException();
    }

    public <A> void lock(long position, long size, boolean shared, A attachment,
                         CompletionHandler<FileLock, ? super A> handler) {
        throw new UnsupportedOperationException();
    }

    public Future<FileLock> lock(long position, long size, boolean shared) {
        throw new UnsupportedOperationException();
    }

    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException();
    }

    public <A> void read(final ByteBuffer dst, final long position, final A attachment,
                         final CompletionHandler<Integer, ? super A> handler) {
        assert dst.isDirect();
        assert dst.position() == 0;

        Runnable action = new Runnable() {
            public void run() {
                try {
                    long id = Native.submitAIORead(epollEventLoop.aioContext, eventFd.intValue(),
                                                   file.intValue(), position, dst.limit(), dst);

                    outstandingRequests.put(id, new IORequest<A>(dst, handler, attachment));

                } catch (IOException e) {
                    handler.failed(e, attachment);
                }
            }
        };

        if (epollEventLoop.inEventLoop()) {
            action.run();
        } else {
            epollEventLoop.submit(action);
        }
    }

    public Future<Integer> read(ByteBuffer dst, long position) {
        final CompletableFuture<Integer> future = new CompletableFuture<Integer>();
        read(dst, position, (Object) null, new CompletionHandler<Integer, Object>() {
            public void completed(Integer result, Object attachment) {
                future.complete(result);
            }

            public void failed(Throwable exc, Object attachment) {
                future.completeExceptionally(exc);
            }
        });

        return future;
    }

    public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        throw new UnsupportedOperationException();
    }

    public Future<Integer> write(ByteBuffer src, long position) {
       throw new UnsupportedOperationException();
    }

    public boolean isOpen() {
        return file.isOpen();
    }

    public void close() throws IOException {
        Runnable close = new Runnable() {
            public void run() {
                try {
                    nettyChannel.doClose();
                } catch (Exception e) {
                    throw new IOError(e);
                }
            }
        };

        if (epollEventLoop.inEventLoop()) {
            close.run();
        } else {
            epollEventLoop.submit(close);
        }
    }

    /**
     * Used as a marker class.
     *
     * Since file IO doesn't fit simply into pipeline approach
     * We only register the file handle and manage the rest in
     * AIOContext. See {@link EpollEventLoop#processReady(EpollEventArray, int)}
     */
     class EventFileChannel extends AbstractEpollChannel {

        EventFileChannel(AIOEpollFileChannel aioChannel) {
            super(new Socket(aioChannel.eventFd.intValue()), Native.EPOLLIN | Native.EPOLLET);
            this.eventLoop = epollEventLoop;
        }

        public void processReady() {
            try {
                long numReady = Native.eventFdRead(eventFd.intValue());
                long[] ids = Native.getAIOEvents(epollEventLoop.aioContext, numReady);
                assert ids.length == numReady * 2;

                for (int i = 0; i < numReady; i++) {
                    long id = ids[i];
                    long lengthRead = ids[i + (int) numReady];
                    IORequest request = outstandingRequests.remove(id);

                    if (request == null) {
                        throw new IllegalStateException();
                    }

                    int len = (int) lengthRead;
                    assert len == lengthRead;
                    request.buffer.limit(len);
                    request.buffer.position(len);
                    request.handler.completed(len, request.attachment);
                }
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        public EpollChannelConfig config() {
            throw new UnsupportedOperationException();
        }

        protected AbstractEpollUnsafe newUnsafe() {
            return null;
        }

        protected SocketAddress localAddress0() {
            throw new UnsupportedOperationException();
        }

        protected SocketAddress remoteAddress0() {
            throw new UnsupportedOperationException();
        }

        protected void doBind(SocketAddress localAddress) throws Exception {
            throw new UnsupportedOperationException();
        }

        protected void doWrite(ChannelOutboundBuffer in) throws Exception {
            throw new UnsupportedOperationException();
        }
    }
}
