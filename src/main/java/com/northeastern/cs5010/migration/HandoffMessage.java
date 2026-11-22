package com.northeastern.cs5010.migration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/**
 * One message of the handoff protocol, plus its wire codec.
 *
 * <p>Every request carries a requestId and every reply echoes it, so the
 * source can tell the answer to this attempt from a late answer to an earlier
 * attempt for the same actor and epoch.
 *
 * <p>The codec is a versioned binary layout (DataOutputStream) wrapped in
 * Base64 so it travels as text over any Transport. It has no third-party
 * dependency, which keeps the jar runnable on the Pi with plain java -cp.
 *
 * <pre>
 * source                          destination
 *   PREPARE(epoch, state)   --->    validate, hold state, start lease
 *                           <---    PREPARED | REJECT
 *   COMMIT(epoch, buffered) --->    install actor, deliver buffered, epoch+1
 *                           <---    COMMITTED | REJECT
 *   ABORT(epoch)            --->    drop held state
 *   QUERY(epoch)            --->    "did you commit?" used to resolve IN_DOUBT
 *   FORWARD(epoch, payload) --->    route a message that reached an old location
 * </pre>
 */
public record HandoffMessage(Type type, String actorId, String actorType, long epoch, long requestId,
                             String state, List<String> messages, String reason) {

    public enum Type {
        PREPARE, PREPARED, REJECT, COMMIT, COMMITTED, ABORT, QUERY, FORWARD
    }

    private static final int VERSION = 1;
    private static final Type[] TYPES = Type.values();

    public HandoffMessage {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(actorId, "actorId");
        actorType = actorType == null ? "" : actorType;
        state = state == null ? "" : state;
        reason = reason == null ? "" : reason;
        messages = messages == null ? List.of() : List.copyOf(messages);
    }

    public static HandoffMessage prepare(String actorId, String actorType, long epoch, String state) {
        return new HandoffMessage(Type.PREPARE, actorId, actorType, epoch, 0, state, null, null);
    }

    public static HandoffMessage prepared(String actorId, long epoch) {
        return new HandoffMessage(Type.PREPARED, actorId, null, epoch, 0, null, null, null);
    }

    public static HandoffMessage reject(String actorId, long epoch, String reason) {
        return new HandoffMessage(Type.REJECT, actorId, null, epoch, 0, null, null, reason);
    }

    public static HandoffMessage commit(String actorId, long epoch, List<String> buffered) {
        return new HandoffMessage(Type.COMMIT, actorId, null, epoch, 0, null, buffered, null);
    }

    public static HandoffMessage committed(String actorId, long epoch) {
        return new HandoffMessage(Type.COMMITTED, actorId, null, epoch, 0, null, null, null);
    }

    public static HandoffMessage abort(String actorId, long epoch) {
        return new HandoffMessage(Type.ABORT, actorId, null, epoch, 0, null, null, null);
    }

    public static HandoffMessage query(String actorId, long epoch) {
        return new HandoffMessage(Type.QUERY, actorId, null, epoch, 0, null, null, null);
    }

    public static HandoffMessage forward(String actorId, long epoch, String payload) {
        return new HandoffMessage(Type.FORWARD, actorId, null, epoch, 0, null, List.of(payload), null);
    }

    /** A copy of this message tagged with the given request id. */
    public HandoffMessage withRequestId(long id) {
        return new HandoffMessage(type, actorId, actorType, epoch, id, state, messages, reason);
    }

    /** A reply carrying the same request id as this request. */
    public HandoffMessage answer(HandoffMessage reply) {
        return reply.withRequestId(requestId);
    }

    public String encode() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(VERSION);
            out.writeByte(type.ordinal());
            writeString(out, actorId);
            writeString(out, actorType);
            out.writeLong(epoch);
            out.writeLong(requestId);
            writeString(out, state);
            out.writeInt(messages.size());
            for (String m : messages) {
                writeString(out, m);
            }
            writeString(out, reason);
        } catch (IOException e) {
            throw new UncheckedIOException(e); // cannot happen on an in-memory stream
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }

    /** Parses a message; throws IllegalArgumentException for anything malformed. */
    public static HandoffMessage decode(String encoded) {
        byte[] raw = Base64.getDecoder().decode(encoded);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw))) {
            int version = in.readUnsignedByte();
            if (version != VERSION) {
                throw new IllegalArgumentException("unsupported handoff version " + version);
            }
            int typeIndex = in.readUnsignedByte();
            if (typeIndex >= TYPES.length) {
                throw new IllegalArgumentException("unknown handoff type " + typeIndex);
            }
            String actorId = readString(in, raw.length);
            String actorType = readString(in, raw.length);
            long epoch = in.readLong();
            long requestId = in.readLong();
            String state = readString(in, raw.length);
            int count = in.readInt();
            if (count < 0 || count > raw.length) {
                throw new IllegalArgumentException("bad message count " + count);
            }
            List<String> messages = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                messages.add(readString(in, raw.length));
            }
            String reason = readString(in, raw.length);
            return new HandoffMessage(TYPES[typeIndex], actorId, actorType, epoch, requestId, state, messages,
                reason);
        } catch (IOException e) {
            throw new IllegalArgumentException("truncated handoff message", e);
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in, int limit) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > limit) {
            throw new IllegalArgumentException("bad string length " + len);
        }
        byte[] b = new byte[len];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
