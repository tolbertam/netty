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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.tuple.ImmutablePair;

import io.netty.util.SuppressForbidden;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.internal.PlatformDependent;
import sun.misc.SharedSecrets;


public class LibAIOTest {

    @BeforeClass
    public static void setup() {
        System.setProperty("netty.aio.maxConcurrency", "1");
    }

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

            AIOContext aio = Native.createAIOContext(1, 1);

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
        Collection<Exception> errors = epollTriggeredReadTest(1024, 16, 8);
        Assert.assertTrue(errors.isEmpty());
    }

    @Test
    public void epollTriggeredReadTestHigherConcurrency() throws IOException, InterruptedException, ExecutionException {
        int old = EpollEventLoop.aioMaxConcurrency;
        try {
            EpollEventLoop.aioMaxConcurrency = 128;
            Collection<Exception> errors = epollTriggeredReadTest(1024, 16, 8);
            Assert.assertTrue(errors.isEmpty());
        } finally {
            EpollEventLoop.aioMaxConcurrency = old;
        }
    }

    @Test
    public void epollTriggeredReadTestLowMaxPending() throws IOException, InterruptedException, ExecutionException {
        int oldConcurrency = EpollEventLoop.aioMaxConcurrency;
        int oldPedning = EpollEventLoop.aioPerLoopMaxPending;

        try {
            EpollEventLoop.aioMaxConcurrency = 128;
            EpollEventLoop.aioPerLoopMaxPending = 32;
            Collection<Exception> errors = epollTriggeredReadTest(1024, 8, 4);
            Assert.assertFalse(errors.isEmpty()); // some requests should have failed with "Too many pending requests"
            for (Exception error : errors) {
                Assert.assertNotNull(error);
                Assert.assertTrue(error.getMessage().contains("Too many pending requests"));
            }
        } finally {
            EpollEventLoop.aioMaxConcurrency = oldConcurrency;
            EpollEventLoop.aioPerLoopMaxPending = oldPedning;
        }
    }

    private Collection<Exception> epollTriggeredReadTest(final int requests, final int numThreads, final int numLoops)
            throws IOException, InterruptedException {
        final int LEN = 65536;
        final EventLoopGroup group = new EpollEventLoopGroup(numLoops, true);
        final EpollEventLoop[] loops = new EpollEventLoop[numLoops];
        for (int i = 0; i < numLoops; i++) {
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
        for (int i = 0; i < requests; i++) {
            write.append(value);
        }

        write.flush();
        write.close();

        final AtomicReference<Throwable> err = new AtomicReference<Throwable>();
        ExecutorService es = Executors.newFixedThreadPool(numThreads);
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final BlockingQueue<Exception> errors = new LinkedBlockingQueue<Exception>();

        for (int i = 0; i < numThreads; i++) {
            final int tid = i;
            es.submit(new Runnable() {
                public void run() {
                    int idx = 0;
                    try {
                        final EpollEventLoop loop = loops[tid % numLoops];

                        AsynchronousFileChannel fc = new AIOEpollFileChannel(file, loop, FileDescriptor.O_RDONLY |
                                                                             FileDescriptor.O_DIRECT);
                        List<ImmutablePair<ByteBuffer, Future<Integer>>> futures =
                            new ArrayList<ImmutablePair<ByteBuffer, Future<Integer>>>();

                        for (int i = 0; i < requests; i++) {
                            //System.err.println(loop.threadProperties().name());
                            ByteBuffer buf = allocateAlignedByteBuffer(LEN, 512);

                            buf.clear();
                            futures.add(new ImmutablePair<ByteBuffer,
                                                         Future<Integer>>(buf, fc.read(buf,
                                                                                       value.length() *
                                                                                       (i % requests))));
                        }

                        for (int i = 0; i < requests; i++) {
                            try {
                                int len = futures.get(i).getRight().get();
                                ByteBuffer buf = futures.get(i).getLeft();
                                buf.flip();
                                Assert.assertEquals(buf.position(), 0);
                                Assert.assertEquals(buf.limit(), len);

                                byte[] output = new byte[value.length()];
                                buf.get(output);
                                Assert.assertEquals(value, new String(output));
                            } catch (Exception ex) {
                                errors.add(ex); // exceptions when completing the future are handled by the callers
                            }
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

        return errors;
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
