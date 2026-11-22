package com.northeastern.cs5010.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** The frame decoder with large frames and arbitrary read sizes. */
class LargeFrameTest {

    @Test
    void randomFramesSplitIntoRandomReadsDecodeExactly() {
        for (long seed = 1; seed <= 5; seed++) {
            Random random = new Random(seed);
            List<String> sent = new ArrayList<>();
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            for (int i = 0; i < 200; i++) {
                int size = i % 50 == 7 ? 1_500_000 + random.nextInt(500_000) : random.nextInt(20_000);
                StringBuilder msg = new StringBuilder(size);
                for (int j = 0; j < size; j++) {
                    msg.append((char) ('a' + random.nextInt(26)));
                }
                sent.add("node" + i + "\n" + msg);
                stream.writeBytes(FramedTransport.frame("node" + i, msg.toString()));
            }
            byte[] all = stream.toByteArray();
            FramedTransport.FrameDecoder decoder = new FramedTransport.FrameDecoder();
            List<String> received = new ArrayList<>();
            int at = 0;
            while (at < all.length) {
                int chunk = Math.min(all.length - at, 1 + random.nextInt(random.nextBoolean() ? 64 : 40_000));
                decoder.feed(ByteBuffer.wrap(all, at, chunk));
                at += chunk;
                for (String body = decoder.next(); body != null; body = decoder.next()) {
                    received.add(body);
                }
            }
            assertEquals(sent.size(), received.size(), "seed " + seed);
            for (int i = 0; i < sent.size(); i++) {
                assertEquals(sent.get(i), received.get(i), "seed " + seed + " frame " + i);
            }
            assertNull(decoder.next(), "nothing left over");
        }
    }

    @Test
    void manyFramesInOneReadAreAllDecoded() {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        for (int i = 0; i < 1_000; i++) {
            stream.writeBytes(FramedTransport.frame("a", Integer.toString(i)));
        }
        FramedTransport.FrameDecoder decoder = new FramedTransport.FrameDecoder();
        decoder.feed(ByteBuffer.wrap(stream.toByteArray()));
        for (int i = 0; i < 1_000; i++) {
            assertEquals("a\n" + i, decoder.next());
        }
        assertNull(decoder.next());
    }

    @Test
    void anEightMegabyteLimitAllowsAOneMegabyteActorAndRejectsCorruptLengths() {
        assertTrue(FramedTransport.MAX_FRAME >= 4 * 1024 * 1024);
        String big = "x".repeat(3_000_000);
        byte[] frame = FramedTransport.frame("pi", big);
        FramedTransport.FrameDecoder decoder = new FramedTransport.FrameDecoder();
        for (int at = 0; at < frame.length; at += 8_192) {
            assertNull(decoder.next());
            decoder.feed(ByteBuffer.wrap(frame, at, Math.min(8_192, frame.length - at)));
        }
        assertEquals("pi\n" + big, decoder.next());

        FramedTransport.FrameDecoder corrupt = new FramedTransport.FrameDecoder();
        corrupt.feed(ByteBuffer.allocate(4).putInt(0, FramedTransport.MAX_FRAME + 1));
        assertThrows(IllegalStateException.class, corrupt::next);
    }

    @Test
    void aMultiMegabyteMessageCrossesARealConnection() throws Exception {
        TCPTransport a = new TCPTransport("a", 0);
        TCPTransport b = new TCPTransport("b", 0);
        try {
            a.start();
            b.start();
            String big = "y".repeat(2_000_000);
            a.send("127.0.0.1:" + b.boundPort(), big);
            a.send("127.0.0.1:" + b.boundPort(), "after");
            assertEquals(big, b.receive("a"));
            assertEquals("after", b.receive("a"));
        } finally {
            a.close();
            b.close();
        }
    }
}
