package minikafka;

import minikafka.audit.AuditLog;
import minikafka.config.BrokerProperties;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

//Starts real brokers without Spring (same constructor Spring would call) and small helpers for the tests
public final class TestSupport {
    private TestSupport() {}

    public static final class Node {
        public final Broker broker;
        public final AuditLog audit;
        public final BrokerProperties props;
        public final Path dir;

        Node(Broker broker, AuditLog audit, BrokerProperties props, Path dir) {
            this.broker = broker; this.audit = audit; this.props = props; this.dir = dir;
        }

        public int port() { return broker.getPort(); }
        public Path auditFile() { return props.getAudit().getFile(); }

        public List<String> auditLines() throws IOException {
            return Files.exists(auditFile()) ? Files.readAllLines(auditFile()) : List.of();
        }

        public void stop() throws IOException {
            broker.stop();
            audit.close();
        }
    }

    //A standalone broker on a random port; 'tweak' may change any setting before it starts
    public static Node start(Path dir, String nodeId, Consumer<BrokerProperties> tweak) throws IOException {
        BrokerProperties p = new BrokerProperties();
        p.setPort(0);
        p.setNodeId(nodeId);
        p.setDataDir(dir.resolve("data"));
        p.getAudit().setFile(dir.resolve("logs").resolve("audit.log"));
        p.getAudit().setPayloadPreview(30);
        if (tweak != null) tweak.accept(p);
        BrokerProperties.Audit a = p.getAudit();
        AuditLog audit = !a.isEnabled() ? AuditLog.disabled()
                : new AuditLog(a.getFile(), nodeId, 10L * 1024 * 1024, 3, a.getPayloadPreview(),
                        a.isConsumePerRecord(), a.getMemoryEvents());
        Broker b = new Broker(p, audit);
        b.start();
        return new Node(b, audit, p, dir);
    }

    public static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    public static void await(String what, long timeoutMs, BooleanSupplier condition) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            try {
                if (condition.getAsBoolean()) return;
            } catch (RuntimeException ignored) {
                // not ready yet
            }
            Thread.sleep(25);
        }
        throw new AssertionError("timed out after " + timeoutMs + " ms waiting for: " + what);
    }
}
