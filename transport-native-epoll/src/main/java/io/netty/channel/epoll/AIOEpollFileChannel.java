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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.Socket;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.LongObjectMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;


public class AIOEpollFileChannel extends AsynchronousFileChannel {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AIOEpollFileChannel.class);

    private final File fileObject;
    private final FileDescriptor file;
    private final FileDescriptor eventFd;
    private final EpollEventLoop epollEventLoop;
    private final EventFileChannel nettyChannel;

    public AIOEpollFileChannel(File file, EpollEventLoop eventLoop, int flags) throws IOException {
        this.fileObject = file;

        if (flags != 0 && flags != FileDescriptor.O_DIRECT) {
            throw new IllegalArgumentException("Only supports read-only files");
        }
        this.file = FileDescriptor.from(file, flags);
        this.eventFd = Native.newEventFd();
        this.epollEventLoop = eventLoop;
        this.nettyChannel = new EventFileChannel(this);

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

    public int getEventFd() {
        return eventFd.intValue();
    }

    public int getFd() {
        return file.intValue();
    }

    public File getFileObject() {
        return fileObject;
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
        if (!dst.isDirect()) {
            handler.failed(new IllegalArgumentException("ByteBuffer is not direct"), attachment);
            return;
        }

        if (dst.position() != 0) {
            handler.failed(new IllegalArgumentException("ByteBuffer position must be 0"), attachment);
            return;
        }

        if (position < 0) {
            handler.failed(new IllegalArgumentException("Position must be >= 0"), attachment);
        }

        Runnable action = new Runnable() {
            public void run() {
                epollEventLoop.aioContext.read(AIOEpollFileChannel.this, dst, position, attachment, handler);
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
            epollEventLoop.aioContext.processReady(AIOEpollFileChannel.this);
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
