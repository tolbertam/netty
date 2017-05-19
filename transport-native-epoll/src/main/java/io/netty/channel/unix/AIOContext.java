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
package io.netty.channel.unix;


import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.Map;

import io.netty.channel.epoll.AIOEpollFileChannel;
import io.netty.channel.epoll.Native;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

public class AIOContext {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AIOContext.class);

    private final long address;
    private long nextId;
    private final Map<Long, IORequest> outstandingRequests;

    public AIOContext(long address) {
        this.address = address;
        this.outstandingRequests = new LongObjectHashMap<IORequest>(128);
        this.nextId = 0;
    }

    public long getNextId() {
        return nextId++;
    }

    public long getAddress() {
        return address;
    }

    public <A> void read(final AIOEpollFileChannel file, final ByteBuffer dst, final long position, final A attachment,
                         final CompletionHandler<Integer, ? super A> handler) {
        assert dst.isDirect();
        assert dst.position() == 0;

        try {
            final int length = (dst.limit() & 511) == 0 ? dst.limit() : ((dst.limit() + 511) & ~511);
            if (dst.capacity() < length) {
                throw new RuntimeException("supplied buffer isn't long enough to handle read length alignment");
            }

            if ((position & 511) != 0) {
                throw new IOException("Read position must be aligned to sector size (usually 512)");
            }

            long id = Native.submitAIORead(this, file.getEventFd(), file.getFd(), position, length, dst);

            IORequest r = outstandingRequests.putIfAbsent(id, new IORequest<A>(dst, position, length, handler,
                                                                               attachment));
            if (r != null) {
                handler.failed(new RuntimeException("ID already found: " + id), attachment);
            }
        } catch (Throwable e) {
            logger.error("Error reading " + file.getFileObject().getAbsolutePath() + "@" + position, e);
            handler.failed(e, attachment);
        }
    }

    public void processReady(AIOEpollFileChannel file) {
        IORequest request = null;
        try {
            long numReady = Native.eventFdRead(file.getEventFd());

            //Shouldn't happen
            if (numReady == 0) {
                return;
            }

            long[] ids = Native.getAIOEvents(this, numReady);
            assert ids.length == numReady * 2;
            for (int i = 0; i < numReady; i++) {
                long id = ids[i];
                long lengthRead = ids[i + (int) numReady];
                request = outstandingRequests.remove(id);
                if (request == null) {
                    throw new IllegalStateException("" + id + " missing");
                }

                int len = (int) lengthRead;
                assert len == lengthRead;
                assert len >= 0 : len;
                request.buffer.limit(len);
                request.buffer.position(len);
                request.handler.completed(len, request.attachment);
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
        public final ByteBuffer buffer;
        public final CompletionHandler handler;
        public final A attachment;
        public final long position;
        public final int length;

        IORequest(ByteBuffer buffer, long position, int length, CompletionHandler handler, A attachment) {
            this.buffer = buffer;
            this.position = position;
            this.length = length;
            this.handler = handler;
            this.attachment = attachment;
        }
    }
}
