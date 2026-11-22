package com.northeastern.cs5010.transport;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLContext;

/**
 * Generates throwaway keystores for TLS tests with the JDK's own keytool, so
 * no certificate files need to be committed.
 *
 * <ul>
 *   <li>cluster.p12: node key signed by the test CA, plus the CA as trusted</li>
 *   <li>outsider.p12: self-signed key, trusts nothing from the cluster</li>
 *   <li>intruder.p12: self-signed key, but trusts the cluster CA, so it can
 *       verify a cluster server while the server must still reject it</li>
 * </ul>
 */
public final class TlsFixtures {

    public static final String PASSWORD = "changeit";
    private static Path dir;

    private TlsFixtures() {
    }

    public static synchronized Path dir() throws Exception {
        if (dir != null) {
            return dir;
        }
        Path d = Files.createTempDirectory("edgeactors-certs");
        genKey(d, "ca.p12", "ca", "CN=Test CA", "-ext", "bc:c");
        keytool(d, "-exportcert", "-rfc", "-alias", "ca", "-keystore", "ca.p12", "-storepass", PASSWORD,
            "-file", "ca.pem");

        genKey(d, "cluster.p12", "node", "CN=node");
        keytool(d, "-certreq", "-alias", "node", "-keystore", "cluster.p12", "-storepass", PASSWORD,
            "-file", "node.csr");
        keytool(d, "-gencert", "-alias", "ca", "-keystore", "ca.p12", "-storepass", PASSWORD,
            "-infile", "node.csr", "-outfile", "node.pem", "-rfc", "-validity", "30",
            "-ext", "ku:c=digitalSignature", "-ext", "eku=serverAuth,clientAuth");
        trustCa(d, "cluster.p12");
        keytool(d, "-importcert", "-alias", "node", "-file", "node.pem", "-keystore", "cluster.p12",
            "-storepass", PASSWORD);

        genKey(d, "outsider.p12", "node", "CN=outsider");
        genKey(d, "intruder.p12", "node", "CN=intruder");
        trustCa(d, "intruder.p12");
        dir = d;
        return d;
    }

    public static SSLContext context(String keystore) throws Exception {
        return TlsContexts.fromKeystore(dir().resolve(keystore), PASSWORD.toCharArray());
    }

    private static void genKey(Path d, String store, String alias, String dname, String... extra)
            throws Exception {
        List<String> args = new ArrayList<>(List.of("-genkeypair", "-alias", alias, "-keyalg", "EC",
            "-groupname", "secp256r1", "-dname", dname, "-validity", "30", "-storetype", "PKCS12",
            "-keystore", store, "-storepass", PASSWORD, "-keypass", PASSWORD));
        args.addAll(List.of(extra));
        keytool(d, args.toArray(String[]::new));
    }

    private static void trustCa(Path d, String store) throws Exception {
        keytool(d, "-importcert", "-noprompt", "-alias", "ca", "-file", "ca.pem", "-keystore", store,
            "-storepass", PASSWORD);
    }

    private static void keytool(Path d, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd).directory(d.toFile()).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) {
            throw new IllegalStateException("keytool " + args[0] + " failed: " + out);
        }
    }
}
