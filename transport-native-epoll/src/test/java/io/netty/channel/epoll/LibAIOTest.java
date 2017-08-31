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
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.util.SuppressForbidden;
import org.junit.Assert;
import org.junit.Test;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.internal.PlatformDependent;
import sun.misc.SharedSecrets;


public class LibAIOTest {

    @Test
    public void globalFlagTest() {
        Epoll.ensureAvailability();
        Assert.assertTrue(Aio.isAvailable());
    }

    @Test
    @SuppressForbidden(reason = "to test native aio reads")
    public void nativeReadTest() throws IOException, InterruptedException {
        EventLoopGroup group = new EpollEventLoopGroup(1);
        File file = File.createTempFile("netty-aio", null);
        file.deleteOnExit();

        String value = "AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJKKKKLLLLMMMMNNNNOOOOPPPPQQQQRRRR" +
                       "SSSSTTTUUUUVVVVWWWWXXXXYYYYZZZZ";

        int len = 0;
        FileWriter write = new FileWriter(file);
        for (int i = 0; i < 1024; i++) {
            write.append(value);
            len += value.length();
        }

        write.flush();
        write.close();
        FileInputStream fileReader = new FileInputStream(file);

        EpollEventLoop loop = (EpollEventLoop) group.next();

            AIOContext aio = Native.createAIOContext(1);

            try {
            ByteBuffer buf = allocateAlignedByteBuffer(65536, 512);

            long id = Native.submitAIORead(aio, loop.eventFd.intValue(),
                                           SharedSecrets.getJavaIOFileDescriptorAccess().get(fileReader.getFD()),
                                           0, 65536, buf);

            long[] ids = Native.getAIOEvents(aio, 1);

            Assert.assertEquals(id, ids[0]);
            byte[] output = new byte[value.length()];
            buf.get(output);
            Assert.assertEquals(value, new String(output));

            group.shutdownGracefully();
        } finally {
            aio.destroy();
        }
    }

    @Test
    public void epollTriggeredReadTest() throws IOException, InterruptedException, ExecutionException {
        final int LEN = 65536;
        final EventLoopGroup group = new EpollEventLoopGroup(8, true);
        final EpollEventLoop[] loops = new EpollEventLoop[8];
        for (int i = 0; i < 8; i++) {
            loops[i] = (EpollEventLoop) group.next();
        }

        final File file = File.createTempFile("netty-aio", null);
        file.deleteOnExit();

        //Make value 1 page
        String tmp = "AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJKKKKLLLLMMMMNNNNOOOOPPPPQQQQRRRR" +
                       "SSSSTTTUUUUVVVVWWWWXXXXYYYYZZZZ";

        while (tmp.length() < LEN) {
            tmp += tmp;
        }

        final String value = tmp.substring(0, LEN);

        FileWriter write = new FileWriter(file);
        for (int i = 0; i < 1024; i++) {
            write.append(value);
        }

        write.flush();
        write.close();

        int THREADS = 8;
        final AtomicReference<Throwable> err = new AtomicReference<Throwable>();
        ExecutorService es = Executors.newFixedThreadPool(THREADS);
        final CountDownLatch latch = new CountDownLatch(THREADS);

        for (int i = 0; i < THREADS; i++) {
            final int tid = i;
            es.submit(new Runnable() {
                public void run() {
                    int idx = 0;
                    try {
                        final EpollEventLoop loop = loops[tid % 8];

                        AsynchronousFileChannel fc = new AIOEpollFileChannel(file, loop, FileDescriptor.O_RDONLY |
                                                                             FileDescriptor.O_DIRECT);
                        List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

                        for (int i = 0; i < 1024; i++) {
                            //System.err.println(loop.threadProperties().name());
                            ByteBuffer buf = allocateAlignedByteBuffer(LEN, 512);

                            buf.clear();
                            futures.add(fc.read(buf, value.length() * (i % 1024)));
                        }

                        for (int i = 0; i < 1024; i++) {
                            futures.get(i).get();
                        }
                    } catch (Throwable t) {
                        System.err.println("Thread: " + tid + ", Index: " + idx);
                        t.printStackTrace();
                        err.set(t);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();
        Assert.assertNull(err.get());
        group.shutdownGracefully();
    }

    public static ByteBuffer allocateAlignedByteBuffer(int capacity, long align) {
        // Power of 2 --> single bit, none power of 2 alignments are not allowed.
        if (Long.bitCount(align) != 1) {
            throw new IllegalArgumentException("Alignment must be a power of 2");
        }
        // We over allocate by the alignment so we know we can have a large enough aligned
        // block of memory to use. Also set order to native while we are here.
        ByteBuffer buffy = PlatformDependent.allocateDirectNoCleaner((int) (capacity + align));
        long address = PlatformDependent.directBufferAddress(buffy);
        // check if we got lucky and the address is already aligned
        if ((address & (align - 1)) == 0) {
            // set the new limit to intended capacity
            buffy.limit(capacity);
            // the slice is now an aligned buffer of the required capacity
            return buffy.slice().order(ByteOrder.nativeOrder());
        } else {
            // we need to shift the start position to an aligned address --> address + (align - (address % align))
            // the modulo replacement with the & trick is valid for power of 2 values only
            int newPosition = (int) (align - (address & (align - 1)));
            // change the position
            buffy.position(newPosition);
            int newLimit = newPosition + capacity;
            // set the new limit to accomodate offset + intended capacity
            buffy.limit(newLimit);
            // the slice is now an aligned buffer of the required capacity
            return buffy.slice().order(ByteOrder.nativeOrder());
        }
    }
}
