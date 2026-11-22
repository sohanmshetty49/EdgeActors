package com.northeastern.cs5010.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Non-blocking TCP transport. One Selector thread accepts every inbound
 * connection and reads frames, so the node needs one thread for networking no
 * matter how many peers connect. That matters on a 4-core Raspberry Pi.
 * Outbound connections are pooled, one SocketChannel per destination.
 */
public class TCPTransport extends FramedTransport {

    private static final System.Logger LOG = System.getLogger(TCPTransport.class.getName());

    private final int port;
    private final Map<String, SocketChannel> outbound = new ConcurrentHashMap<>();
    private volatile boolean running;
    private Selector selector;
    private ServerSocketChannel server;
    private Thread loop;

    /** Port 0 asks the OS for a free port; read it back with {@link #boundPort()}. */
    public TCPTransport(String nodeName, int port) {
        super(nodeName);
        this.port = port;
    }

    @Override
    public synchronized void start() throws IOException {
        selector = Selector.open();
        server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(port));
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);
        running = true;
        loop = new Thread(this::eventLoop, nodeName + "-selector");
        loop.setDaemon(true);
        loop.start();
    }

    /** The port actually bound, useful when the transport was created with port 0. */
    public int boundPort() throws IOException {
        return ((InetSocketAddress) server.getLocalAddress()).getPort();
    }

    private void eventLoop() {
        try {
            while (running) {
                try {
                    selector.select(100);
                    Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                    while (it.hasNext()) {
                        SelectionKey key = it.next();
                        it.remove();
                        if (!key.isValid()) {
                            continue;
                        }
                        if (key.isAcceptable()) {
                            accept();
                        } else if (key.isReadable()) {
                            read(key);
                        }
                    }
                } catch (IOException e) {
                    LOG.log(System.Logger.Level.WARNING, "selector loop error", e);
                }
            }
        } catch (ClosedSelectorException e) {
            // closed underneath us; nothing left to clean up
        } finally {
            shutdownSelector();
        }
    }

    /**
     * Runs on the selector thread when the loop ends. Closing the selector alone
     * would leave every accepted connection open, so peers could keep writing
     * into a node that is gone. Close each channel first.
     */
    private void shutdownSelector() {
        try {
            for (SelectionKey key : selector.keys()) {
                key.channel().close();
            }
            selector.close();
        } catch (IOException | ClosedSelectorException e) {
            LOG.log(System.Logger.Level.DEBUG, "selector already closed", e);
        }
    }

    private void accept() throws IOException {
        SocketChannel ch = server.accept();
        if (ch == null) {
            return; // another wakeup already took the pending connection
        }
        ch.configureBlocking(false);
        ch.register(selector, SelectionKey.OP_READ, new FrameDecoder());
    }

    private void read(SelectionKey key) {
        SocketChannel ch = (SocketChannel) key.channel();
        FrameDecoder decoder = (FrameDecoder) key.attachment();
        ByteBuffer buf = ByteBuffer.allocate(8192);
        try {
            int n = ch.read(buf);
            if (n < 0) {
                closeConnection(key);
                return;
            }
            buf.flip();
            decoder.feed(buf);
            String frame;
            while ((frame = decoder.next()) != null) {
                deliver(frame);
            }
        } catch (IOException | RuntimeException e) {
            // Bad frame or dropped peer: drop this connection only, keep the node alive.
            closeConnection(key);
        }
    }

    private void closeConnection(SelectionKey key) {
        key.cancel();
        try {
            key.channel().close();
        } catch (IOException ignored) {
            // already closing
        }
    }

    @Override
    public void send(String destination, String message) throws IOException {
        ByteBuffer out = ByteBuffer.wrap(frame(nodeName, message));
        SocketChannel ch = connectionTo(destination);
        try {
            synchronized (ch) {
                while (out.hasRemaining()) {
                    ch.write(out);
                }
            }
        } catch (IOException e) {
            // Forget the broken connection so the next send reconnects.
            outbound.remove(destination, ch);
            ch.close();
            throw e;
        }
        recordSent();
    }

    private synchronized SocketChannel connectionTo(String destination) throws IOException {
        SocketChannel existing = outbound.get(destination);
        if (existing != null && existing.isOpen()) {
            return existing;
        }
        HostPort target = parseAddress(destination);
        SocketChannel ch = SocketChannel.open(new InetSocketAddress(target.host(), target.port()));
        outbound.put(destination, ch);
        return ch;
    }

    @Override
    public boolean isConnected() {
        return running;
    }

    /**
     * Stops the node: no new connections, every outbound and inbound connection
     * closed. Waits for the selector thread so the port and sockets are really
     * released when this returns.
     */
    @Override
    public synchronized void close() {
        running = false;
        try {
            if (server != null) {
                server.close();
            }
            for (SocketChannel ch : outbound.values()) {
                ch.close();
            }
            outbound.clear();
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "error while closing transport", e);
        }
        if (loop != null) {
            selector.wakeup();
            if (Thread.currentThread() != loop) {
                try {
                    loop.join(2_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
