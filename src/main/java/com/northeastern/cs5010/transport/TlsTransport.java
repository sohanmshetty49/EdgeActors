package com.northeastern.cs5010.transport;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

/**
 * Mutual TLS 1.3 transport between edge and cloud nodes.
 *
 * <p>Both sides present a certificate and both sides verify the other against
 * the cluster trust store, so a device that does not hold a cluster
 * certificate cannot send or receive actor traffic.
 *
 * <p>Each connection is served by a Java 21 virtual thread. Virtual threads
 * park cheaply on blocking reads, so this keeps the simple blocking TLS API
 * without paying one OS thread per peer, which is what made thread-per-
 * connection too expensive on a Raspberry Pi before Java 21.
 */
public class TlsTransport extends FramedTransport {

    private static final System.Logger LOG = System.getLogger(TlsTransport.class.getName());
    private static final String[] PROTOCOLS = {"TLSv1.3"};

    private final int port;
    private final SSLContext context;
    private final Map<String, SSLSocket> outbound = new ConcurrentHashMap<>();
    private final Set<Socket> inbound = ConcurrentHashMap.newKeySet();
    private volatile boolean running;
    private SSLServerSocket server;

    /** Port 0 asks the OS for a free port; read it back with {@link #boundPort()}. */
    public TlsTransport(String nodeName, int port, SSLContext context) {
        super(nodeName);
        if (context == null) {
            throw new IllegalArgumentException("TLS context is required");
        }
        this.port = port;
        this.context = context;
    }

    @Override
    public synchronized void start() throws IOException {
        server = (SSLServerSocket) context.getServerSocketFactory().createServerSocket(port);
        server.setEnabledProtocols(PROTOCOLS);
        server.setNeedClientAuth(true);
        running = true;
        Thread.ofVirtual().name(nodeName + "-tls-accept").start(this::acceptLoop);
    }

    public int boundPort() {
        return server.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = server.accept();
                inbound.add(socket);
                Thread.ofVirtual().name(nodeName + "-tls-conn").start(() -> readLoop(socket));
            } catch (IOException e) {
                if (running) {
                    LOG.log(System.Logger.Level.WARNING, "accept failed", e);
                }
            }
        }
    }

    private void readLoop(Socket socket) {
        // The TLS handshake runs on the first read, so an untrusted peer fails here.
        try (socket; DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {
            while (running) {
                int len = in.readInt();
                if (len < 0 || len > MAX_FRAME) {
                    throw new IOException("bad frame length " + len);
                }
                byte[] body = new byte[len];
                in.readFully(body);
                deliver(new String(body, StandardCharsets.UTF_8));
            }
        } catch (IOException | RuntimeException e) {
            // Peer closed, handshake rejected, or malformed frame: drop this connection only.
        } finally {
            inbound.remove(socket);
        }
    }

    @Override
    public void send(String destination, String message) throws IOException {
        byte[] frame = frame(nodeName, message);
        SSLSocket socket = connectionTo(destination);
        try {
            synchronized (socket) {
                OutputStream out = socket.getOutputStream();
                out.write(frame);
                out.flush();
            }
        } catch (IOException e) {
            // Forget the broken connection so the next send reconnects.
            outbound.remove(destination, socket);
            socket.close();
            throw e;
        }
        recordSent();
    }

    private synchronized SSLSocket connectionTo(String destination) throws IOException {
        SSLSocket existing = outbound.get(destination);
        if (existing != null && !existing.isClosed()) {
            return existing;
        }
        HostPort target = parseAddress(destination);
        SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket(target.host(), target.port());
        socket.setEnabledProtocols(PROTOCOLS);
        try {
            // Handshake now so an untrusted peer is reported to the caller of send().
            socket.startHandshake();
        } catch (IOException e) {
            socket.close();
            throw e;
        }
        outbound.put(destination, socket);
        return socket;
    }

    @Override
    public boolean isConnected() {
        return running;
    }

    @Override
    public synchronized void close() {
        running = false;
        closeQuietly(server);
        outbound.values().forEach(TlsTransport::closeQuietly);
        outbound.clear();
        inbound.forEach(TlsTransport::closeQuietly);
        inbound.clear();
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception ignored) {
            // closing anyway
        }
    }
}
