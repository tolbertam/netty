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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.AIOContext;
import io.netty.channel.unix.Socket;
import io.netty.util.internal.PlatformDependent;
import sun.misc.SharedSecrets;


public class LibAIOTest {

    @Test
    public void nativeReadTest() throws IOException, InterruptedException {
        EventLoopGroup group = new EpollEventLoopGroup(1);
        File file = File.createTempFile("netty-aio", null);
        file.deleteOnExit();

        String value = "AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJKKKKLLLLMMMMNNNNOOOOPPPPQQQQRRRR" +
                       "SSSSTTTUUUUVVVVWWWWXXXXYYYYZZZZ";

        FileWriter write = new FileWriter(file);
        for (int i = 0; i < 1024; i++) {
            write.append(value);
        }

        write.flush();
        write.close();

        FileInputStream fileReader = new FileInputStream(file);

        EpollEventLoop loop = (EpollEventLoop) group.next();

        AIOContext aio = Native.createAIOContext(10);
        ByteBuffer buf = PlatformDependent.allocateDirectNoCleaner(1024);

        long id = Native.submitAIORead(aio, loop.eventFd.intValue(),
                                       SharedSecrets.getJavaIOFileDescriptorAccess().get(fileReader.getFD()),
                                       0, value.length(), buf);

        long[] ids = Native.getAIOEvents(aio, 1);

        Assert.assertEquals(id, ids[0]);
        byte[] output = new byte[value.length()];
        buf.flip();
        buf.get(output);
        Assert.assertEquals(value, new String(output));

        group.shutdownGracefully();
    }

    @Test
    public void epollTriggeredReadTest() throws IOException, InterruptedException, ExecutionException {
        EventLoopGroup group = new EpollEventLoopGroup(1);
        final File file = File.createTempFile("netty-aio", null);
        file.deleteOnExit();

        final String value = "AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJKKKKLLLLMMMMNNNNOOOOPPPPQQQQRRRR" +
                       "SSSSTTTUUUUVVVVWWWWXXXXYYYYZZZZ";

        FileWriter write = new FileWriter(file);
        for (int i = 0; i < 1024; i++) {
            write.append(value);
        }

        write.flush();
        write.close();

        final EpollEventLoop loop = (EpollEventLoop) group.next();

        int THREADS = 8;

        ExecutorService es = Executors.newFixedThreadPool(THREADS);
        final CountDownLatch latch = new CountDownLatch(THREADS);

        for (int i = 0; i < THREADS; i++) {
            es.submit(new Runnable() {
                public void run() {
                    try {
                        AsynchronousFileChannel fc = new AIOEpollFileChannel(file, loop);
                        ByteBuffer buf = PlatformDependent.allocateDirectNoCleaner(1024);

                        for (int i = 0; i < 1024; i++) {
                            buf.clear();
                            Future<Integer> f = fc.read(buf, value.length() * i);
                            int len = f.get();
                            Assert.assertEquals(buf.limit(), len);

                            byte[] output = new byte[value.length()];
                            buf.flip();
                            buf.get(output);
                            Assert.assertEquals(value, new String(output));
                        }
                    } catch (Throwable t) {
                        throw new AssertionError(t);
                    }
                    latch.countDown();
                }
            });
        }

        latch.await();
        group.shutdownGracefully();
    }
}
