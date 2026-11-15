package minikafka;

import lombok.extern.slf4j.Slf4j;
import minikafka.audit.AuditLog;
import minikafka.cluster.ClusterHost;
import minikafka.cluster.ClusterNode;
import minikafka.cluster.ClusterStatus;
import minikafka.cluster.Replicator;
import minikafka.cluster.TopicEnd;
import minikafka.config.BrokerProperties;
import minikafka.group.GroupCoordinator;
import minikafka.group.OffsetStore;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


//Request frame (UTF-8 text header, optional payload after first '\n'):
//
//  Producing and reading
//  HELLO   <clientId>                          -> "OK"          (names this connection in the audit log)
//  PRODUCE <topic> <key>\n<payload>            -> "OK <partition> <offset>"
//  FETCH   <topic> <partition> <offset>        -> "OK <payload>" | "EMPTY"
//  FETCHB  <topic> <partition> <offset> <max>  -> "OK 0 <count>\n" + records            (no group, you track the offset)
//
//  Consumer groups
//  JOIN    <group> <topic> <consumer>          -> "OK <generation> <p1,p2,...|->"
//  POLL    <group> <topic> <consumer> <max>    -> "OK <generation> <count>\n" + records (starts at the group's position)
//  COMMIT  <group> <topic> <consumer> [p:o,..] -> "OK <p:o,...|->"                      (default: current positions)
//  BEAT    <group> <topic> <consumer>          -> "OK <generation>"
//  LEAVE   <group> <topic> <consumer>          -> "OK"
//
//  Between brokers of a cluster (CL_HEARTBEAT, CL_VOTE, CL_REPLICATE, CL_ENDS): see ClusterNode
//
//  A "records" body is, per record:  int partition | long offset | int length | payload bytes
//  Any failure                                 -> "ERR <message>"      (a follower answers "ERR NOT_LEADER <address>")

@Slf4j
@Component
public class Broker implements SmartLifecycle {

    //Topic names become file names, so keep them to a safe character set
    private static final Pattern TOPIC_NAME = Pattern.compile("[A-Za-z0-9._-]{1,200}");
    private static final Pattern CLIENT_ID = Pattern.compile("[A-Za-z0-9._@-]{1,100}");
    private static final Pattern PARTITION_FILE = Pattern.compile("(.+)-\\d+\\.log");
    private static final int MAX_BATCH_RECORDS = 500;
    private static final int MAX_REPLY_BYTES = 8 * 1024 * 1024;       // keep every reply under the 16 MB frame limit

    private final BrokerProperties props;
    private final AuditLog audit;
    private final ClusterNode cluster;               // null when running standalone
    private final Replicator replicator;
    private final OffsetStore offsets;
    private final GroupCoordinator groups;
    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> partitionLocks = new ConcurrentHashMap<>();
    private final Set<Socket> openConnections = ConcurrentHashMap.newKeySet();   // closed on stop(): no zombie connections
    private ExecutorService pool;
    private ServerSocket server;
    private volatile boolean running;
    private boolean stopped;

    public Broker(BrokerProperties props, AuditLog audit) {
        this.props = props;
        this.audit = audit;
        BrokerProperties.Cluster c = props.getCluster();
        if (c.isEnabled()) {
            List<ClusterNode.Peer> peers = c.getPeers().stream().map(ClusterNode.Peer::parse).toList();
            this.cluster = new ClusterNode(new Host(), audit, new ClusterNode.Config(props.getNodeId(), peers,
                    c.getHeartbeatMs(), c.getElectionTimeoutMs(), c.getAckTimeoutMs(),
                    props.getDataDir().resolve("cluster-state.properties")));
            this.replicator = cluster;
        } else {
            this.cluster = null;
            this.replicator = Replicator.STANDALONE;
        }
        try {
            Files.createDirectories(props.getDataDir());
            this.offsets = new OffsetStore(props.getDataDir().resolve(OffsetStore.FILE), replicator);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open " + props.getDataDir(), e);
        }
        this.groups = new GroupCoordinator(new View(), offsets, audit,
                props.getGroup().getSessionTimeoutMs(), System::currentTimeMillis);
    }

    //Lifecycle

    //Starting the broker by creating a pool first and getting the pool properties from Topic.java
    @Override
    public synchronized void start() {
        if (running) return;
        if (stopped) throw new IllegalStateException("this broker was stopped; create a new one");
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
        log.info("MiniKafka broker {} listening on {} (data: {})",
                props.getNodeId(), server.getLocalPort(), props.getDataDir().toAbsolutePath());
        audit.record("NODE_START", props.getNodeId(), "port", String.valueOf(server.getLocalPort()),
                "mode", cluster == null ? "standalone" : "cluster");
        if (cluster != null) {
            String adv = props.getCluster().getAdvertise();
            cluster.start(adv == null || adv.isBlank() ? "localhost:" + server.getLocalPort() : adv);
        }
    }

    @Override
    public synchronized void stop() {
        if (!running) return;
        running = false;
        stopped = true;
        audit.record("NODE_STOP", props.getNodeId());
        try { server.close(); } catch (IOException e) { log.warn("closing server socket", e); }
        pool.shutdownNow();
        for (Socket s : openConnections) {
            try { s.close(); } catch (IOException ignored) { /* already gone */ }
        }
        if (cluster != null) cluster.close();
        topics.values().forEach(t -> {
            try { t.close(); } catch (IOException e) { log.warn("closing topic {}", t.name(), e); }
        });
        topics.clear();
        try { offsets.close(); } catch (IOException e) { log.warn("closing offsets log", e); }
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
        if (name.startsWith("__")) throw new IllegalArgumentException("topic names starting with __ are reserved");
        return open(name);
    }

    //Find or create, without the client-facing name rules (the cluster layer also needs the internal topics)
    private Topic open(String name) {
        //atomic
        return topics.computeIfAbsent(name, n -> {
            try {
                BrokerProperties.GroupCommit gc = props.getGroupCommit();
                return gc.isEnabled()
                        ? new Topic(props.getDataDir(), n, props.getPartitions(), gc.getMaxBatch(), gc.getQueueCapacity())
                        : new Topic(props.getDataDir(), n, props.getPartitions());
            }
            catch (IOException e) { throw new UncheckedIOException(e); }
        });
    }

    private Object partitionLock(String topic, int partition) {
        return partitionLocks.computeIfAbsent(topic + "-" + partition, k -> new Object());
    }

    //Append a record; the partition is chosen by hashing the key. 'actor' = who is asking (for the audit log).
    public ProduceResult produce(String actor, String topicName, String key, byte[] payload) throws IOException {
        replicator.requireLeader();
        Topic t = topic(topicName);
        int part = t.partitionFor(key);
        long off;
        try {
            if (replicator.isClustered()) {
                //In a cluster, append + replicate happen together per partition so followers see offsets in order
                synchronized (partitionLock(topicName, part)) {
                    off = t.append(part, payload);
                    replicator.replicate(topicName, part, off, payload);
                }
            } else {
                off = t.append(part, payload);
            }
        } catch (IOException | RuntimeException e) {
            audit.record("PRODUCE_FAILED", actor, "topic", topicName, "partition", String.valueOf(part),
                    "key", key, "bytes", String.valueOf(payload.length), "error", String.valueOf(e.getMessage()));
            throw e;
        }
        audit.produced(actor, topicName, part, off, key, payload);
        return new ProduceResult(part, off);
    }

    public ProduceResult produce(String topicName, String key, byte[] payload) throws IOException {
        return produce("internal", topicName, key, payload);
    }

    //Read one record; empty if nothing has been written (or committed) at that offset yet
    public Optional<byte[]> fetch(String actor, String topicName, int partition, long offset) throws IOException {
        List<Record> r = fetchBatch(actor, topicName, partition, offset, 1);
        return r.isEmpty() ? Optional.empty() : Optional.of(r.get(0).payload());
    }

    public Optional<byte[]> fetch(String topicName, int partition, long offset) throws IOException {
        return fetch("internal", topicName, partition, offset);
    }

    //Read up to max records starting at 'from'. Nothing beyond the high watermark is ever returned.
    public List<Record> fetchBatch(String actor, String topicName, int partition, long from, int max) throws IOException {
        replicator.requireLeader();
        AppendLog log = topic(topicName).partition(partition);
        long hwm = replicator.highWatermark(topicName, partition, log.nextOffset());
        List<Record> recs = log.readBatch(from, Math.min(Math.max(max, 1), MAX_BATCH_RECORDS), hwm);
        List<Record> out = new ArrayList<>();
        long bytes = 0;
        for (Record r : recs) {
            if (!out.isEmpty() && bytes + r.payload().length > MAX_REPLY_BYTES) break;
            out.add(r);
            bytes += r.payload().length;
        }
        audit.consumed(actor, null, null, topicName, partition, out);
        return out;
    }

    //Topic names found on disk plus those opened in this run (internal topics are hidden)
    public List<String> listTopics() throws IOException {
        Set<String> names = new TreeSet<>(topics.keySet());
        try (var files = Files.list(props.getDataDir())) {
            files.map(f -> f.getFileName().toString()).forEach(n -> {
                Matcher m = PARTITION_FILE.matcher(n);
                if (m.matches() && TOPIC_NAME.matcher(m.group(1)).matches()) names.add(m.group(1));
            });
        } catch (NoSuchFileException ignored) {
            // no data directory yet
        }
        names.removeIf(n -> n.startsWith("__"));
        return List.copyOf(names);
    }

    //Committed end offset (next offset to be written) of every partition of a topic
    public long[] endOffsets(String topicName) {
        replicator.requireLeader();
        Topic t = topic(topicName);
        long[] ends = new long[t.numPartitions()];
        for (int i = 0; i < ends.length; i++)
            ends[i] = replicator.highWatermark(topicName, i, t.partition(i).nextOffset());
        return ends;
    }

    //Consumer groups (every call must go to the leader, because group state lives there)

    public GroupCoordinator.JoinResult joinGroup(String actor, String group, String topic, String consumer) throws IOException {
        replicator.requireLeader();
        return groups.join(group, topic, consumer, actor);
    }

    public void leaveGroup(String actor, String group, String topic, String consumer) {
        replicator.requireLeader();
        groups.leave(group, topic, consumer, actor);
    }

    public int heartbeat(String group, String topic, String consumer) {
        replicator.requireLeader();
        return groups.heartbeat(group, topic, consumer);
    }

    public GroupCoordinator.PollResult poll(String actor, String group, String topic, String consumer, int max) throws IOException {
        replicator.requireLeader();
        return groups.poll(group, topic, consumer, Math.min(Math.max(max, 1), MAX_BATCH_RECORDS), actor);
    }

    public Map<Integer, Long> commit(String actor, String group, String topic, String consumer,
                                     Map<Integer, Long> explicitOffsets) throws IOException {
        replicator.requireLeader();
        return groups.commit(group, topic, consumer, explicitOffsets, actor);
    }

    public GroupCoordinator.GroupInfo describeGroup(String group) throws IOException {
        replicator.requireLeader();
        return groups.describe(group);
    }

    //Node-local view of the logs: ignores leadership and the high watermark. For diagnostics and tests.
    public long[] localEndOffsets(String topicName) {
        Topic t = topic(topicName);
        long[] ends = new long[t.numPartitions()];
        for (int i = 0; i < ends.length; i++) ends[i] = t.partition(i).nextOffset();
        return ends;
    }

    public List<Record> localRecords(String topicName, int partition, long from, int max) throws IOException {
        return topic(topicName).partition(partition).readBatch(from, max);
    }

    public GroupCoordinator groups() { return groups; }
    public AuditLog audit() { return audit; }

    public ClusterStatus clusterStatus() {
        return cluster == null ? ClusterStatus.standalone(props.getNodeId()) : cluster.status();
    }

    //TCP protocol

    //Who is on the other end of one TCP connection (for the audit log)
    static final class Session {
        final String remote;
        volatile String clientId;
        boolean announced;
        long requests;

        Session(String remote) { this.remote = remote; }

        String actor() { return "tcp:" + (clientId == null ? "anonymous" : clientId) + "@" + remote; }
    }

    private void handle(Socket s) {
        String remote = String.valueOf(s.getRemoteSocketAddress()).replaceFirst("^/", "");
        Session ses = new Session(remote);
        openConnections.add(s);
        try (s;
             var in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
             var out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {
            while (true) {
                byte[] req;
                try { req = Frames.read(in); } catch (EOFException e) { return; }
                if (!running) return;
                Frames.send(out, process(req, ses));
            }
        } catch (IOException ignored) {
            // client went away
        } finally {
            openConnections.remove(s);
            if (ses.announced) audit.record("DISCONNECT", ses.actor(), "requests", String.valueOf(ses.requests));
        }
    }

    byte[] process(byte[] req) {
        return process(req, new Session("local"));
    }

    //50 52 4F 44 55 43 45 20  6F 72 64 65 72 73 20  61 6C 69 63 65  0A  64 65 70 6F 73 69 74
    //P  R  O  D  U  C  E  ' ' o  r  d  e  r  s  ' '  a  l  i  c  e   \n  d  e  p  o  s  i  t
    byte[] process(byte[] req, Session ses) {
        try {
            int nl = -1;
            for (int i = 0; i < req.length; i++)
            {
                if (req[i] == '\n')
                {
                    nl = i; break;
                }
            }
            String header = new String(req, 0, nl < 0 ? req.length : nl, StandardCharsets.UTF_8);
            String[] p = header.split(" ");
            if (!p[0].startsWith("CL_")) {                      // broker-to-broker traffic is not "a client"
                ses.requests++;
                if (!ses.announced) {
                    ses.announced = true;
                    audit.record("CONNECT", ses.actor());
                }
            }
            switch (p[0]) {
                case "HELLO" ->
                {
                    if (p.length < 2 || !CLIENT_ID.matcher(p[1]).matches())
                        throw new IllegalArgumentException("HELLO needs a client id made of letters, digits, . _ @ -");
                    ses.clientId = p[1];
                    audit.record("HELLO", ses.actor());
                    return text("OK");
                }
                case "PRODUCE" ->
                {
                    byte[] payload = nl < 0 ? new byte[0] : Arrays.copyOfRange(req, nl + 1, req.length);
                    ProduceResult r = produce(ses.actor(), p[1], p.length > 2 ? p[2] : "", payload);
                    return text("OK " + r.partition() + " " + r.offset());
                }
                case "FETCH" ->
                {
                    Optional<byte[]> r = fetch(ses.actor(), p[1], Integer.parseInt(p[2]), Long.parseLong(p[3]));
                    if (r.isEmpty()) return text("EMPTY");
                    byte[] head = text("OK ");
                    byte[] out = new byte[head.length + r.get().length];
                    System.arraycopy(head, 0, out, 0, head.length);
                    System.arraycopy(r.get(), 0, out, head.length, r.get().length);
                    return out;
                }
                case "FETCHB" ->
                {
                    int partition = Integer.parseInt(p[2]);
                    List<Record> recs = fetchBatch(ses.actor(), p[1], partition, Long.parseLong(p[3]), Integer.parseInt(p[4]));
                    List<GroupCoordinator.PolledRecord> out = new ArrayList<>();
                    for (Record r : recs) out.add(new GroupCoordinator.PolledRecord(partition, r.offset(), r.payload()));
                    return encodeRecords(0, out);
                }
                case "JOIN" ->
                {
                    GroupCoordinator.JoinResult r = joinGroup(ses.actor(), p[1], p[2], p[3]);
                    return text("OK " + r.generation() + " " + csv(r.partitions()));
                }
                case "LEAVE" ->
                {
                    leaveGroup(ses.actor(), p[1], p[2], p[3]);
                    return text("OK");
                }
                case "BEAT" -> { return text("OK " + heartbeat(p[1], p[2], p[3])); }
                case "POLL" ->
                {
                    GroupCoordinator.PollResult r = poll(ses.actor(), p[1], p[2], p[3], Integer.parseInt(p[4]));
                    return encodeRecords(r.generation(), r.records());
                }
                case "COMMIT" ->
                {
                    Map<Integer, Long> explicit = null;
                    if (p.length > 4 && !p[4].equals("-")) {
                        explicit = new TreeMap<>();
                        for (String pair : p[4].split(",")) {
                            String[] kv = pair.split(":");
                            explicit.put(Integer.parseInt(kv[0]), Long.parseLong(kv[1]));
                        }
                    }
                    Map<Integer, Long> done = commit(ses.actor(), p[1], p[2], p[3], explicit);
                    if (done.isEmpty()) return text("OK -");
                    StringJoiner sj = new StringJoiner(",");
                    done.forEach((k, v) -> sj.add(k + ":" + v));
                    return text("OK " + sj);
                }
                case "CL_HEARTBEAT", "CL_VOTE", "CL_REPLICATE", "CL_ENDS" ->
                {
                    if (cluster == null) return text("ERR cluster mode is not enabled on this node");
                    byte[] body = nl < 0 ? new byte[0] : Arrays.copyOfRange(req, nl + 1, req.length);
                    return text(cluster.handle(p, body));
                }
                default -> { return text("ERR unknown command"); }
            }
        } catch (Exception e) {
            return text("ERR " + e.getMessage());
        }
    }

    private static byte[] text(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    private static String csv(List<Integer> ints) {
        if (ints.isEmpty()) return "-";
        StringJoiner sj = new StringJoiner(",");
        for (int i : ints) sj.add(String.valueOf(i));
        return sj.toString();
    }

    //"OK <generation> <count>\n" followed by, for each record: int partition, long offset, int length, payload
    private static byte[] encodeRecords(int generation, List<GroupCoordinator.PolledRecord> recs) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.write(text("OK " + generation + " " + recs.size() + "\n"));
            for (GroupCoordinator.PolledRecord r : recs) {
                out.writeInt(r.partition());
                out.writeLong(r.offset());
                out.writeInt(r.payload().length);
                out.write(r.payload());
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);              // cannot happen for an in-memory stream
        }
    }

    //What the group coordinator sees: topics and partitions, capped at the high watermark
    private final class View implements GroupCoordinator.Partitions {
        @Override public int partitionCount(String topic) {
            replicator.requireLeader();
            return topic(topic).numPartitions();
        }

        @Override public List<Record> read(String topic, int partition, long from, int max) throws IOException {
            AppendLog log = open(topic).partition(partition);
            return log.readBatch(from, max, replicator.highWatermark(topic, partition, log.nextOffset()));
        }

        @Override public long endOffset(String topic, int partition) {
            AppendLog log = open(topic).partition(partition);
            return replicator.highWatermark(topic, partition, log.nextOffset());
        }
    }

    //What the cluster layer sees: the raw local logs, including the internal offsets log
    private final class Host implements ClusterHost {
        private boolean internal(String topic) { return OffsetStore.TOPIC.equals(topic); }

        private AppendLog log(String topic, int partition) {
            if (!TOPIC_NAME.matcher(topic).matches()) throw new IllegalArgumentException("invalid topic name");
            return open(topic).partition(partition);
        }

        @Override public long endOffset(String topic, int partition) {
            return internal(topic) ? offsets.endOffset() : log(topic, partition).nextOffset();
        }

        @Override public List<Record> readLocal(String topic, int partition, long from, int max) throws IOException {
            return internal(topic) ? offsets.read(from, max) : log(topic, partition).readBatch(from, max);
        }

        @Override public long appendReplica(String topic, int partition, long expected, byte[] payload) throws IOException {
            return internal(topic) ? offsets.appendReplica(expected, payload)
                    : log(topic, partition).appendIfNext(expected, payload);
        }

        @Override public void truncate(String topic, int partition, long toOffset) throws IOException {
            if (internal(topic)) offsets.truncateTo(toOffset);
            else log(topic, partition).truncateTo(toOffset);
        }

        @Override public List<TopicEnd> snapshotEnds() throws IOException {
            List<TopicEnd> out = new ArrayList<>();
            for (String t : listTopics()) {
                Topic tp = open(t);
                for (int i = 0; i < tp.numPartitions(); i++) out.add(new TopicEnd(t, i, tp.partition(i).nextOffset()));
            }
            out.add(new TopicEnd(OffsetStore.TOPIC, 0, offsets.endOffset()));
            return out;
        }

        @Override public void onLeadershipChange(boolean leader) {
            groups.reset();                  // group membership and positions belong to one leadership; start clean
        }
    }
}
