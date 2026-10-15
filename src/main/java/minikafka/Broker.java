package minikafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import minikafka.config.BrokerProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;


//Request frame (UTF-8 text header, optional payload after first '\n'):
//PRODUCE <topic> <key>\n<payload>   -> "OK <partition> <offset>"
//FETCH   <topic> <partition> <offset> -> "OK <payload>" | "EMPTY"
//Any failure                         -> "ERR <message>"

@Slf4j
@Component
@RequiredArgsConstructor
public class Broker implements SmartLifecycle {

    //Topic names become file names, so keep them to a safe character set
    private static final Pattern TOPIC_NAME = Pattern.compile("[A-Za-z0-9._-]{1,200}");

    private final BrokerProperties props;
    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private ExecutorService pool;
    private ServerSocket server;
    private volatile boolean running;

    //Lifecycle

    //Starting the broker by creating a pool first and getting the pool properties from Topic.java
    @Override
    public synchronized void start() {
        if (running) return;
        try {
            pool = Executors.newFixedThreadPool(props.getWorkerThreads());
            server = new ServerSocket(props.getPort());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Cannot bind broker port " + props.getPort(), e);
        }
        running = true;
        Thread acceptor = new Thread(this::acceptLoop, "acceptor");
        acceptor.setDaemon(true);
        acceptor.start();
        log.info("MiniKafka broker listening on {} (data: {})",
                server.getLocalPort(), props.getDataDir().toAbsolutePath());
    }

    @Override
    public synchronized void stop() {
        if (!running) return;
        running = false;
        try { server.close(); } catch (IOException e) { log.warn("closing server socket", e); }
        pool.shutdownNow();
        topics.values().forEach(t -> {
            try { t.close(); } catch (IOException e) { log.warn("closing topic {}", t.name(), e); }
        });
        topics.clear();
        log.info("MiniKafka broker stopped");
    }

    @Override
    public boolean isRunning() { return running; }

    //The actual bound TCP port (useful when configured with port 0)
    public int getPort() { return server.getLocalPort(); }

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = server.accept();
                pool.submit(() -> handle(s));
            } catch (IOException e) {
                if (running) log.error("accept failed", e);
            }
        }
    }

    //core operations

    private Topic topic(String name) {

        if (!TOPIC_NAME.matcher(name).matches())
        {
            throw new IllegalArgumentException("invalid topic name");
        }
        return topics.computeIfAbsent(name, n -> {
            try { return new Topic(props.getDataDir(), n, props.getPartitions()); }
            catch (IOException e) { throw new UncheckedIOException(e); }
        });
    }

    //Append a record; the partition is chosen by hashing the key
    public ProduceResult produce(String topicName, String key, byte[] payload) throws IOException {
        Topic t = topic(topicName);
        int part = t.partitionFor(key);
        long off = t.partition(part).append(payload);
        return new ProduceResult(part, off);
    }

    //Read one record; empty if nothing has been written at that offset yet
    public Optional<byte[]> fetch(String topicName, int partition, long offset) throws IOException {
        Record r = topic(topicName).partition(partition).read(offset);
        return Optional.ofNullable(r).map(Record::payload);
    }

    //TCP protocol

    private void handle(Socket s) {
        try (
             var in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
             var out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {
            while (true) {
                byte[] req;
                try { req = Frames.read(in); } catch (EOFException e) { return; }
                Frames.send(out, process(req));
            }
        } catch (IOException ignored) {
            // client went away
        }
    }

    byte[] process(byte[] req) {
        try {
            int nl = -1;
            for (int i = 0; i < req.length; i++) if (req[i] == '\n') { nl = i; break; }
            String header = new String(req, 0, nl < 0 ? req.length : nl, StandardCharsets.UTF_8);
            String[] p = header.split(" ");
            switch (p[0]) {
                case "PRODUCE" -> {
                    byte[] payload = nl < 0 ? new byte[0] : Arrays.copyOfRange(req, nl + 1, req.length);
                    ProduceResult r = produce(p[1], p.length > 2 ? p[2] : "", payload);
                    return ("OK " + r.partition() + " " + r.offset()).getBytes(StandardCharsets.UTF_8);
                }
                case "FETCH" -> {
                    Optional<byte[]> r = fetch(p[1], Integer.parseInt(p[2]), Long.parseLong(p[3]));
                    if (r.isEmpty()) return "EMPTY".getBytes(StandardCharsets.UTF_8);
                    byte[] head = "OK ".getBytes(StandardCharsets.UTF_8);
                    byte[] out = new byte[head.length + r.get().length];
                    System.arraycopy(head, 0, out, 0, head.length);
                    System.arraycopy(r.get(), 0, out, head.length, r.get().length);
                    return out;
                }
                default -> { return "ERR unknown command".getBytes(StandardCharsets.UTF_8); }
            }
        } catch (Exception e) {
            return ("ERR " + e.getMessage()).getBytes(StandardCharsets.UTF_8);
        }
    }
}
