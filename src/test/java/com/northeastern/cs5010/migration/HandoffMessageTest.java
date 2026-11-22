package com.northeastern.cs5010.migration;

import static org.junit.jupiter.api.Assertions.*;

import com.northeastern.cs5010.migration.HandoffMessage.Type;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The wire codec must round-trip every message and reject anything malformed. */
class HandoffMessageTest {

    @Test
    void everyFactoryRoundTrips() {
        List<HandoffMessage> all = List.of(
            HandoffMessage.prepare("a", "counter", 4, "1,2,3,4"),
            HandoffMessage.prepared("a", 4),
            HandoffMessage.reject("a", 4, "no room"),
            HandoffMessage.commit("a", 4, List.of("5", "6", "7")),
            HandoffMessage.committed("a", 4),
            HandoffMessage.abort("a", 4),
            HandoffMessage.query("a", 4),
            HandoffMessage.forward("a", 5, "payload"));
        for (HandoffMessage m : all) {
            assertEquals(m, HandoffMessage.decode(m.encode()), m.type().name());
        }
        assertEquals(Type.values().length, all.size(), "one factory per message type");
    }

    @Test
    void requestIdTravelsAndRepliesEchoIt() {
        HandoffMessage request = HandoffMessage.prepare("a", "counter", 2, "s").withRequestId(42);
        assertEquals(42, HandoffMessage.decode(request.encode()).requestId());
        HandoffMessage reply = request.answer(HandoffMessage.prepared("a", 2));
        assertEquals(Type.PREPARED, reply.type());
        assertEquals(42, reply.requestId());
        assertEquals(0, HandoffMessage.prepared("a", 2).requestId());
    }

    @Test
    void nullsBecomeEmptyAndListsAreCopied() {
        HandoffMessage m = new HandoffMessage(Type.ABORT, "a", null, 0, 0, null, null, null);
        assertEquals("", m.actorType());
        assertEquals("", m.state());
        assertEquals("", m.reason());
        assertEquals(List.of(), m.messages());
        assertThrows(UnsupportedOperationException.class, () -> m.messages().add("x"));
        assertThrows(NullPointerException.class, () -> new HandoffMessage(null, "a", null, 0, 0, null, null, null));
        assertThrows(NullPointerException.class, () -> new HandoffMessage(Type.ABORT, null, null, 0, 0, null, null, null));
    }

    @Test
    void unicodeNewlinesAndLargeStateSurvive() {
        String state = "line1\nline2 é中文 " + "x".repeat(200_000);
        HandoffMessage m = HandoffMessage.prepare("actor\nid", "type", Long.MAX_VALUE, state);
        HandoffMessage back = HandoffMessage.decode(m.encode());
        assertEquals(state, back.state());
        assertEquals("actor\nid", back.actorId());
        assertEquals(Long.MAX_VALUE, back.epoch());
    }

    @Test
    void rejectsNonBase64() {
        assertThrows(IllegalArgumentException.class, () -> HandoffMessage.decode("%%%"));
    }

    @Test
    void rejectsUnknownVersion() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> HandoffMessage.decode(encode(out -> {
            out.writeByte(99);
            out.writeByte(0);
        })));
    }

    @Test
    void rejectsUnknownType() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> HandoffMessage.decode(encode(out -> {
            out.writeByte(1);
            out.writeByte(200);
        })));
    }

    @Test
    void rejectsTruncatedMessage() {
        String full = HandoffMessage.prepare("a", "t", 1, "state").encode();
        byte[] raw = Base64.getDecoder().decode(full);
        byte[] cut = java.util.Arrays.copyOf(raw, raw.length - 3);
        assertThrows(IllegalArgumentException.class,
            () -> HandoffMessage.decode(Base64.getEncoder().encodeToString(cut)));
    }

    @Test
    void rejectsImpossibleStringLength() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> HandoffMessage.decode(encode(out -> {
            out.writeByte(1);
            out.writeByte(0);
            out.writeInt(-1);
        })));
        assertThrows(IllegalArgumentException.class, () -> HandoffMessage.decode(encode(out -> {
            out.writeByte(1);
            out.writeByte(0);
            out.writeInt(Integer.MAX_VALUE);
        })));
    }

    @Test
    void rejectsImpossibleMessageCount() throws IOException {
        for (int count : new int[] {-5, Integer.MAX_VALUE}) {
            assertThrows(IllegalArgumentException.class, () -> HandoffMessage.decode(encode(out -> {
                out.writeByte(1);
                out.writeByte(Type.COMMIT.ordinal());
                writeString(out, "a");
                writeString(out, "");
                out.writeLong(0); // epoch
                out.writeLong(0); // requestId
                writeString(out, "");
                out.writeInt(count);
            })));
        }
    }

    private interface Body {
        void write(DataOutputStream out) throws IOException;
    }

    private static String encode(Body body) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            body.write(out);
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }
}
