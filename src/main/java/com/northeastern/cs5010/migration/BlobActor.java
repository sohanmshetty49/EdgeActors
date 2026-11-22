package com.northeastern.cs5010.migration;

/**
 * Migratable actor whose state is a block of text of a chosen size. Used to
 * measure migration pause time as a function of state size (1 KB, 100 KB,
 * 1 MB) on the Pi and the cloud node.
 *
 * <p>"fill N" replaces the state with N bytes of ASCII text; any other
 * message is counted. The snapshot is the count, a colon, then the text.
 */
public final class BlobActor implements MigratableActor {

    public static final String TYPE = "blob";
    private static final String PATTERN = "EdgeActors-migration-state-";

    private long count;
    private String data = "";

    @Override
    public void onMessage(String payload) {
        if (payload.startsWith("fill ")) {
            data = text(Integer.parseInt(payload.substring(5).trim()));
            return;
        }
        count++;
    }

    @Override
    public String snapshot() {
        return count + ":" + data;
    }

    @Override
    public void restore(String state) {
        int colon = state.indexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("bad blob state");
        }
        count = Long.parseLong(state.substring(0, colon));
        data = state.substring(colon + 1);
    }

    public int size() {
        return data.length();
    }

    public long count() {
        return count;
    }

    /** n bytes of repeating ASCII text. */
    static String text(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("size must not be negative");
        }
        StringBuilder out = new StringBuilder(n);
        while (out.length() < n) {
            out.append(PATTERN, 0, Math.min(PATTERN.length(), n - out.length()));
        }
        return out.toString();
    }
}
