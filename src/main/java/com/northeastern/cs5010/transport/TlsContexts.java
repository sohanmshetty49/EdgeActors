package com.northeastern.cs5010.transport;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/** Builds the SSLContext a node uses for mutual TLS. */
public final class TlsContexts {

    private TlsContexts() {
    }

    /**
     * Loads one PKCS12 keystore that holds this node's private key and the
     * cluster CA certificate. The key proves who this node is; the CA entry
     * decides which peers it accepts. scripts/gen-cluster-certs.sh creates
     * keystores in exactly this shape.
     */
    public static SSLContext fromKeystore(Path keystore, char[] password)
            throws IOException, GeneralSecurityException {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystore)) {
            store.load(in, password);
        }
        KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, password);
        TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(store);
        SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(keys.getKeyManagers(), trust.getTrustManagers(), null);
        return context;
    }
}
