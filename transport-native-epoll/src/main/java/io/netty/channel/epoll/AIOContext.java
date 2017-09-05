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


import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.ArrayDeque;
import java.util.Map;

import io.netty.channel.ChannelException;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

public class AIOContext {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AIOContext.class);

    private final long address;
    private long nextId;
    private long outstanding;
    private long maxOutstanding;
    private final Map<Long, IORequest> outstandingRequests;
    private final ArrayDeque<IORequest> pendingRequests;
    private volatile boolean destroyed;

    AIOContext(long address, long maxOutstanding) {
        this.address = address;
        this.outstanding = 0;
        this.maxOutstanding = maxOutstanding;
        this.outstandingRequests = new LongObjectHashMap<IORequest>(128);
        this.pendingRequests = new ArrayDeque<IORequest>(1 << 16);
        this.nextId = 0;
    }

    public void destroy() {
        if (destroyed) {
            return;
        }

        destroyed = true;
        Native.destroyAIOContext(this);
    }

    public long getNextId() {
        return nextId++;
    }

    public long getAddress() {
        return address;
    }

    public <A> void read(final AIOEpollFileChannel file, final ByteBuffer dst, final long position, final A attachment,
                         final CompletionHandler<Integer, ? super A> handler) {
        assert !destroyed;
        assert dst.isDirect();
        assert dst.position() == 0;
        assert file.epollEventLoop.aioContext == this;

        int length = (dst.limit() & 511) == 0 ? dst.limit() : ((dst.limit() + 511) & ~511);
        if (dst.capacity() < length) {
            handler.failed(new RuntimeException("supplied buffer isn't long enough to handle read length " +
                                                "alignment"), attachment);
            return;
        }

        if ((position & 511) != 0) {
            handler.failed(new IOException("Read position must be aligned to sector size (usually 512)"),
                           attachment);
            return;
        }

        IORequest<A> request = new IORequest<A>(file, dst, position, length, handler, attachment);

        try {
            // Avoid sending overflowing the aio context queue (EGAIN)
            // instead buffer locally
            if (outstanding + 1 <= maxOutstanding) {
                long id = Native.submitAIORead(this, file.getEventFd(), file.getFd(), position, length, dst);

                IORequest r = outstandingRequests.putIfAbsent(id, request);
                ++outstanding;
                if (r != null) {
                    handler.failed(new RuntimeException("ID already found: " + id), attachment);
                }
            } else {
                if (!pendingRequests.offer(request)) {
                    handler.failed(new RuntimeException("Too many pending requests"), attachment);
                }
            }
        } catch (Throwable e) {
            if (e instanceof ChannelException && e.getMessage().contains("Resource temporarily unavailable")) {
                if (!pendingRequests.offer(request)) {
                    handler.failed(new RuntimeException("Too many pending requests"), attachment);
                }
            } else {
                logger.error("Error reading " + file.getFileObject().getAbsolutePath() + "@" + position, e);
                handler.failed(e, attachment);
            }
        }
    }

    public void processReady(AIOEpollFileChannel file) {
        assert !destroyed;
        assert file.epollEventLoop.aioContext == this;
        IORequest request = null;
        try {
            long numReady = Native.eventFdRead(file.getEventFd());

            //Shouldn't happen
            if (numReady == 0) {
                return;
            }

            //Process the finished reads
            long[] ids = Native.getAIOEvents(this, numReady);
            assert ids.length == numReady * 2;
            for (int i = 0; i < numReady; i++) {
                long id = ids[i];
                long lengthRead = ids[i + (int) numReady];
                request = outstandingRequests.remove(id);
                if (request == null) {
                    throw new IllegalStateException("" + id + " missing");
                }

                // Finally complete the request
                int len = (int) lengthRead;
                assert len == lengthRead;
                assert len >= 0 : len;
                request.buffer.limit(len);
                request.buffer.position(len);
                request.handler.completed(len, request.attachment);
            }

            //add any pending reads
            //We do this after processing the finished reads incase any outstanding reads
            //they share the same read buffer
            for (int i = 0; i < numReady; i++) {
                --outstanding;
                IORequest req = null;
                // Push any pending requests if we have room
                try {
                    if (outstanding + 1 <= maxOutstanding && (req = pendingRequests.poll()) != null) {
                        assert req.file.epollEventLoop.aioContext == this;
                        long nextId = Native.submitAIORead(this, req.file.getEventFd(), req.file.getFd(),
                                                           req.position, req.length, req.buffer);
                        IORequest r = outstandingRequests.putIfAbsent(nextId, req);
                        assert r == null;
                        ++outstanding;
                    }
                } catch (ChannelException e) {
                    if (req != null && e.getMessage().contains("Resource temporarily unavailable")) {
                        pendingRequests.addFirst(req);
                    } else {
                        throw e;
                    }
                }
            }

        } catch (IOException e) {
            throw new IOError(e);
        } catch (Throwable t) {
            long position = request == null ? -1 : request.position;
            int length = request == null ? -1 : request.length;
            logger.error("Error reading " + file.getFileObject().getAbsolutePath() + "@" + position + " - " +
                         length, t);
            throw new RuntimeException(t);
        }
    }

    class IORequest<A> {
        public final AIOEpollFileChannel file;
        public final ByteBuffer buffer;
        public final CompletionHandler handler;
        public final A attachment;
        public final long position;
        public final int length;

        IORequest(AIOEpollFileChannel file, ByteBuffer buffer, long position, int length, CompletionHandler handler,
                  A attachment) {
            this.file = file;
            this.buffer = buffer;
            this.position = position;
            this.length = length;
            this.handler = handler;
            this.attachment = attachment;
        }
    }
}
