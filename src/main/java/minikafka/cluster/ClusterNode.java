package minikafka.cluster;

import minikafka.AppendLog;
import minikafka.Frames;
import minikafka.Record;
import minikafka.audit.AuditLog;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

//Replication + leader election ("Raft-lite") over the same framed TCP protocol the clients use.
//
//ONE leader takes all client reads and writes. Followers only copy the leader's logs.
//
//  WRITE   leader appends locally -> sends CL_REPLICATE to every follower in parallel ->
//          answers the client only when a MAJORITY (leader included) has the record.
//          Majority overlap is what guarantees a record that was acknowledged survives a leader crash.
//  ELECT   a follower that hears no heartbeat for a (randomised) election timeout starts an election:
//          term+1, votes for itself, asks the others. A node grants at most ONE vote per term and only to a
//          candidate whose logs are at least as long as its own. A majority of votes makes a leader.
//  HEAL    a leader checks every follower after taking over: missing records are pushed, and records on which
//          the follower disagrees (written by an old leader that never got a majority) are overwritten.
//
//Differences from real Raft, on purpose, to keep it readable:
//  * there is no per-record term; "is this log up to date" compares total records, and "do two logs agree" compares
//    the CRC of the record before the one being sent
//  * a record that failed to reach a majority stays in the old leader's log and is committed later if it ends up
//    in the new leader's log (the client was told "failed", so it may retry: at-least-once)
//  * no membership changes, no snapshots
public final class ClusterNode implements Replicator, Closeable {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    public record Peer(String id, String host, int port) {
        public String address() { return host + ":" + port; }

        //"node-2=localhost:9093"
        public static Peer parse(String spec) {
            int eq = spec.indexOf('='), colon = spec.lastIndexOf(':');
            if (eq <= 0 || colon <= eq + 1)
                throw new IllegalArgumentException("peer must look like id=host:port, got: " + spec);
            return new Peer(spec.substring(0, eq).trim(), spec.substring(eq + 1, colon).trim(),
                    Integer.parseInt(spec.substring(colon + 1).trim()));
        }
    }

    public record Config(String nodeId, List<Peer> peers, long heartbeatMs, long electionTimeoutMs,
                         long ackTimeoutMs, Path stateFile) {}

    private static final System.Logger LOG = System.getLogger("minikafka.cluster");

    private final ClusterHost host;
    private final AuditLog audit;
    private final Config cfg;
    private final Map<String, Link> links = new LinkedHashMap<>();
    private final Random random = new Random();

    // state guarded by 'lock' (network calls are NEVER made while holding it)
    private final Object lock = new Object();
    private Role role = Role.FOLLOWER;
    private long currentTerm;
    private String votedFor;
    private String leaderId;
    private String leaderAddr;
    private long leaderSince;
    private String selfAddr = "?";

    private volatile long electionDeadline;
    private volatile long lastHeartbeatSent;
    private volatile boolean closed;
    private final Object applyLock = new Object();                      // followers apply one replicate at a time
    private final ConcurrentHashMap<String, Long> hw = new ConcurrentHashMap<>();   // committed end per partition (leader only)
    private ScheduledExecutorService ticker;
    private ExecutorService rpc;

    public ClusterNode(ClusterHost host, AuditLog audit, Config cfg) {
        this.host = host;
        this.audit = audit;
        this.cfg = cfg;
        for (Peer p : cfg.peers()) links.put(p.id(), new Link(p));
        loadState();
    }

    // ================================================================== lifecycle

    public void start(String advertisedAddress) {
        synchronized (lock) {
            selfAddr = advertisedAddress;
            resetElectionTimerLocked();
            if (links.isEmpty()) electionDeadline = System.currentTimeMillis();   // single node: no one to wait for
        }
        rpc = Executors.newCachedThreadPool(daemon("cluster-rpc"));
        ticker = Executors.newSingleThreadScheduledExecutor(daemon("cluster-ticker"));
        long period = Math.max(10, cfg.heartbeatMs() / 3);
        ticker.scheduleWithFixedDelay(this::tick, period, period, TimeUnit.MILLISECONDS);
        audit.record("CLUSTER_START", cfg.nodeId(), "address", advertisedAddress,
                "peers", String.valueOf(links.keySet()), "term", String.valueOf(currentTerm));
    }

    @Override
    public void close() {
        closed = true;
        if (ticker != null) ticker.shutdownNow();
        if (rpc != null) rpc.shutdownNow();
        for (Link l : links.values()) l.close();
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }

    // ================================================================== Replicator (called by the broker)

    @Override public boolean isClustered() { return true; }

    @Override
    public void requireLeader() {
        synchronized (lock) {
            if (role != Role.LEADER) throw new NotLeaderException(leaderAddr);
        }
    }

    @Override
    public long highWatermark(String topic, int partition, long localEnd) {
        return hw.getOrDefault(key(topic, partition), 0L);
    }

    @Override
    public void replicate(String topic, int partition, long offset, byte[] payload) throws IOException {
        final long term;
        synchronized (lock) {
            if (role != Role.LEADER) throw new NotLeaderException(leaderAddr);
            term = currentTerm;
        }
        int needFromPeers = majority() - 1;
        if (needFromPeers > 0) {
            CompletionService<Boolean> cs = new ExecutorCompletionService<>(rpc);
            for (Link l : links.values()) cs.submit(() -> push(l, term, topic, partition, offset, payload));
            int acks = 0, done = 0;
            long deadline = System.currentTimeMillis() + cfg.ackTimeoutMs();
            try {
                while (done < links.size() && acks < needFromPeers) {
                    long wait = deadline - System.currentTimeMillis();
                    if (wait <= 0) break;
                    Future<Boolean> f = cs.poll(wait, TimeUnit.MILLISECONDS);
                    if (f == null) break;
                    done++;
                    try {
                        if (f.get()) acks++;
                    } catch (ExecutionException ignored) {
                        // that follower failed; counted as no ack
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while waiting for replicas");
            }
            if (acks < needFromPeers) {
                requireLeader();                          // maybe we were deposed meanwhile
                throw new ReplicationException(topic + "-" + partition + "@" + offset + " reached only "
                        + (acks + 1) + " of " + (links.size() + 1) + " nodes (majority is " + majority() + ")");
            }
        }
        hw.merge(key(topic, partition), offset + 1, Math::max);
    }

    // ================================================================== leader side: pushing records

    private boolean push(Link l, long term, String topic, int partition, long offset, byte[] payload) {
        try {
            String r = l.call(replicateHeader(term, topic, partition, offset, prevCrcToken(topic, partition, offset)), payload);
            String[] t = r.split(" ");
            switch (t[0]) {
                case "OK" -> {
                    l.lastOk = System.currentTimeMillis();
                    return true;
                }
                case "STALE" -> {
                    stepDown(Long.parseLong(t[1]));
                    return false;
                }
                case "GAP", "CONFLICT" -> {
                    return syncPeer(l, term, topic, partition, offset + 1, Long.parseLong(t[1]));
                }
                default -> {
                    return false;
                }
            }
        } catch (IOException | RuntimeException e) {
            l.inSync = false;
            return false;
        }
    }

    //Bring one follower partition up to 'targetEnd', starting at 'from'.
    //GAP      -> follower is behind: restart from its end
    //CONFLICT -> follower's previous record differs from ours: step back one record and try again
    //           (the follower truncates at the first record that differs and takes ours from there on)
    private boolean syncPeer(Link l, long term, String topic, int partition, long targetEnd, long from) throws IOException {
        long p = Math.max(0, Math.min(from, targetEnd));
        long steps = 0;
        while (p < targetEnd) {
            if (closed || !isLeaderInTerm(term) || ++steps > 1_000_000) return false;
            List<Record> rec = host.readLocal(topic, partition, p, 1);
            if (rec.isEmpty()) return false;
            String r = l.call(replicateHeader(term, topic, partition, p, prevCrcToken(topic, partition, p)),
                    rec.get(0).payload());
            String[] t = r.split(" ");
            switch (t[0]) {
                case "OK" -> p++;
                case "GAP" -> p = Math.min(Long.parseLong(t[1]), p);
                case "CONFLICT" -> {
                    if (p == 0) return false;
                    p--;
                }
                case "STALE" -> {
                    stepDown(Long.parseLong(t[1]));
                    return false;
                }
                default -> {
                    return false;
                }
            }
        }
        l.lastOk = System.currentTimeMillis();
        return true;
    }

    //After taking over (or after a follower reconnects): compare every partition and repair the follower
    private void syncPeerAll(Link l, long term) throws IOException {
        String r = l.call("CL_ENDS " + term + " " + cfg.nodeId() + " " + selfAddr, null);
        String[] lines = r.split("\n");
        String[] first = lines[0].split(" ");
        if (first[0].equals("STALE")) {
            stepDown(Long.parseLong(first[1]));
            return;
        }
        if (!first[0].equals("OK")) return;
        Map<String, Long> followerEnds = new HashMap<>();
        for (int i = 1; i < lines.length; i++) {
            String[] f = lines[i].split(" ");
            if (f.length == 3) followerEnds.put(key(f[0], Integer.parseInt(f[1])), Long.parseLong(f[2]));
        }
        boolean ok = true;
        for (TopicEnd le : host.snapshotEnds()) {
            if (le.end() == 0) continue;
            long fnext = followerEnds.getOrDefault(key(le.topic(), le.partition()), 0L);
            //Behind: push what is missing. Equal or ahead: re-send the last record to verify both logs agree.
            long from = fnext >= le.end() ? le.end() - 1 : fnext;
            ok &= syncPeer(l, term, le.topic(), le.partition(), le.end(), from);
        }
        l.inSync = ok;
        if (ok) audit.record("FOLLOWER_SYNCED", cfg.nodeId(), "follower", l.peer.id(), "term", String.valueOf(term));
    }

    private String replicateHeader(long term, String topic, int partition, long offset, String prevCrc) {
        return "CL_REPLICATE " + term + " " + cfg.nodeId() + " " + selfAddr + " " + topic + " "
                + partition + " " + offset + " " + prevCrc;
    }

    //CRC of the record just before 'offset', or "none" at the start of the log
    private String prevCrcToken(String topic, int partition, long offset) throws IOException {
        if (offset == 0) return "none";
        List<Record> r = host.readLocal(topic, partition, offset - 1, 1);
        if (r.isEmpty()) throw new IOException("leader is missing record " + (offset - 1) + " of " + topic + "-" + partition);
        return String.valueOf(AppendLog.crc32(r.get(0).payload()));
    }

    // ================================================================== ticker: heartbeats and elections

    private void tick() {
        if (closed) return;
        try {
            long now = System.currentTimeMillis();
            Role r;
            long term;
            synchronized (lock) {
                r = role;
                term = currentTerm;
            }
            if (r == Role.LEADER) {
                if (now - lastHeartbeatSent >= cfg.heartbeatMs()) {
                    lastHeartbeatSent = now;
                    heartbeatRound(term);
                }
                checkQuorum(now);
            } else if (now >= electionDeadline) {
                startElection();
            }
        } catch (Throwable t) {
            LOG.log(System.Logger.Level.WARNING, "cluster tick failed: " + t);
        }
    }

    private void heartbeatRound(long term) {
        for (Link l : links.values()) {
            rpc.submit(() -> {
                try {
                    String r = l.call("CL_HEARTBEAT " + term + " " + cfg.nodeId() + " " + selfAddr, null);
                    String[] t = r.split(" ");
                    if (t[0].equals("STALE")) {
                        stepDown(Long.parseLong(t[1]));
                        return;
                    }
                    l.lastOk = System.currentTimeMillis();
                    if (!l.inSync && isLeaderInTerm(term) && l.syncing.compareAndSet(false, true)) {
                        try {
                            syncPeerAll(l, term);
                        } finally {
                            l.syncing.set(false);
                        }
                    }
                } catch (IOException | RuntimeException e) {
                    l.inSync = false;
                }
            });
        }
    }

    //A leader that cannot reach a majority steps down instead of pretending to lead
    private void checkQuorum(long now) {
        int alive = 1;
        for (Link l : links.values()) if (now - l.lastOk < cfg.electionTimeoutMs()) alive++;
        long since, term;
        synchronized (lock) {
            since = leaderSince;
            term = currentTerm;
        }
        if (alive < majority() && now - since > cfg.electionTimeoutMs()) {
            audit.record("QUORUM_LOST", cfg.nodeId(), "term", String.valueOf(term), "reachable", String.valueOf(alive));
            stepDown(term);
        }
    }

    private void startElection() {
        final long term;
        synchronized (lock) {
            if (role == Role.LEADER) return;
            currentTerm++;
            term = currentTerm;
            votedFor = cfg.nodeId();
            role = Role.CANDIDATE;
            leaderId = null;
            leaderAddr = null;
            persistLocked();
            resetElectionTimerLocked();
        }
        audit.record("ELECTION_START", cfg.nodeId(), "term", String.valueOf(term));
        long score;
        try {
            score = score();
        } catch (IOException e) {
            return;
        }
        int need = majority(), grants = 1;
        if (grants < need) {
            CompletionService<String> cs = new ExecutorCompletionService<>(rpc);
            for (Link l : links.values())
                cs.submit(() -> l.call("CL_VOTE " + term + " " + cfg.nodeId() + " " + score, null));
            long deadline = System.currentTimeMillis() + cfg.electionTimeoutMs();
            try {
                for (int i = 0; i < links.size() && grants < need; i++) {
                    long wait = deadline - System.currentTimeMillis();
                    if (wait <= 0) break;
                    Future<String> f = cs.poll(wait, TimeUnit.MILLISECONDS);
                    if (f == null) break;
                    try {
                        String[] t = f.get().split(" ");
                        long theirTerm = Long.parseLong(t[1]);
                        if (theirTerm > term) {
                            stepDown(theirTerm);
                            return;
                        }
                        if (t[0].equals("GRANTED")) grants++;
                    } catch (ExecutionException | RuntimeException ignored) {
                        // unreachable peer: no vote
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (grants >= need) {
            synchronized (lock) {
                if (role == Role.CANDIDATE && currentTerm == term) becomeLeaderLocked(grants);
            }
        }
    }

    private void becomeLeaderLocked(int votes) {
        long now = System.currentTimeMillis();
        role = Role.LEADER;
        leaderId = cfg.nodeId();
        leaderAddr = selfAddr;
        leaderSince = now;
        hw.clear();
        try {
            //Everything this node holds is treated as committed: it won a vote from a majority whose logs it was
            //at least as long as, so it has every record that a majority acknowledged.
            for (TopicEnd e : host.snapshotEnds()) hw.put(key(e.topic(), e.partition()), e.end());
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "could not read local logs while becoming leader: " + e);
        }
        for (Link l : links.values()) {
            l.inSync = false;
            l.lastOk = now;
        }
        lastHeartbeatSent = 0;                           // send heartbeats right away
        host.onLeadershipChange(true);
        audit.record("LEADER_ELECTED", cfg.nodeId(), "term", String.valueOf(currentTerm), "votes", String.valueOf(votes));
    }

    private void becomeFollowerLocked(long term) {
        boolean wasLeader = role == Role.LEADER;
        audit.record("ROLE_CHANGE", cfg.nodeId(), "from", role.name(), "to", "FOLLOWER", "term", String.valueOf(term));
        role = Role.FOLLOWER;
        if (wasLeader) host.onLeadershipChange(false);
    }

    private void stepDown(long newTerm) {
        synchronized (lock) {
            if (newTerm > currentTerm) {
                currentTerm = newTerm;
                votedFor = null;
                persistLocked();
            }
            if (role != Role.FOLLOWER) {
                becomeFollowerLocked(currentTerm);
                leaderId = null;
                leaderAddr = null;
            }
            resetElectionTimerLocked();
        }
    }

    // ================================================================== follower side: handling peer commands

    //Entry point for CL_* commands arriving on the broker's TCP port. p = header split on spaces.
    public String handle(String[] p, byte[] body) throws IOException {
        return switch (p[0]) {
            case "CL_HEARTBEAT" -> onHeartbeat(Long.parseLong(p[1]), p[2], p[3]);
            case "CL_VOTE" -> onVote(Long.parseLong(p[1]), p[2], Long.parseLong(p[3]));
            case "CL_REPLICATE" -> onReplicate(p, body);
            case "CL_ENDS" -> onEnds(Long.parseLong(p[1]), p[2], p[3]);
            default -> "ERR unknown cluster command";
        };
    }

    private String onHeartbeat(long term, String fromLeader, String addr) {
        synchronized (lock) {
            if (term < currentTerm) return "STALE " + currentTerm;
            acceptLeaderLocked(term, fromLeader, addr);
            return "OK " + currentTerm;
        }
    }

    private String onVote(long term, String candidate, long candidateScore) throws IOException {
        long myScore = score();
        synchronized (lock) {
            if (term < currentTerm) return "DENIED " + currentTerm;
            if (term > currentTerm) {                      // a newer term: forget the old vote and any leadership
                currentTerm = term;
                votedFor = null;
                if (role != Role.FOLLOWER) {
                    becomeFollowerLocked(term);
                    leaderId = null;
                    leaderAddr = null;
                }
                persistLocked();
            }
            boolean canVote = votedFor == null || votedFor.equals(candidate);
            if (canVote && candidateScore >= myScore) {
                votedFor = candidate;
                persistLocked();
                resetElectionTimerLocked();                // give the candidate time to win before we start our own
                audit.record("VOTE_GRANTED", cfg.nodeId(), "candidate", candidate, "term", String.valueOf(term));
                return "GRANTED " + currentTerm;
            }
            return "DENIED " + currentTerm;
        }
    }

    private String onReplicate(String[] p, byte[] body) throws IOException {
        long term = Long.parseLong(p[1]);
        String fromLeader = p[2], addr = p[3], topic = p[4], prev = p[7];
        int partition = Integer.parseInt(p[5]);
        long offset = Long.parseLong(p[6]);
        synchronized (lock) {
            if (term < currentTerm) return "STALE " + currentTerm;
            acceptLeaderLocked(term, fromLeader, addr);    // a replicate also counts as a heartbeat
        }
        synchronized (applyLock) {
            long next = host.endOffset(topic, partition);
            if (offset > next) return "GAP " + next;                         // we are missing earlier records
            if (!prevMatches(topic, partition, offset, prev)) return "CONFLICT " + next;
            if (offset < next) {
                List<Record> existing = host.readLocal(topic, partition, offset, 1);
                if (!existing.isEmpty() && Arrays.equals(existing.get(0).payload(), body)) return "OK " + next;   // already have it
                host.truncate(topic, partition, offset);                      // our tail disagrees with the leader: drop it
                audit.record("TRUNCATE", fromLeader, "topic", topic, "partition", String.valueOf(partition),
                        "toOffset", String.valueOf(offset), "reason", "log diverged from leader");
            }
            long off = host.appendReplica(topic, partition, offset, body);
            if (off < 0) return "GAP " + host.endOffset(topic, partition);
            audit.record("REPLICATED", fromLeader, "topic", topic, "partition", String.valueOf(partition),
                    "offset", String.valueOf(off), "bytes", String.valueOf(body.length));
            return "OK " + (off + 1);
        }
    }

    private boolean prevMatches(String topic, int partition, long offset, String prevCrc) throws IOException {
        if (offset == 0) return true;
        if (prevCrc.equals("none")) return false;
        List<Record> r = host.readLocal(topic, partition, offset - 1, 1);
        return !r.isEmpty() && String.valueOf(AppendLog.crc32(r.get(0).payload())).equals(prevCrc);
    }

    private String onEnds(long term, String fromLeader, String addr) throws IOException {
        synchronized (lock) {
            if (term < currentTerm) return "STALE " + currentTerm;
            acceptLeaderLocked(term, fromLeader, addr);
        }
        StringBuilder sb = new StringBuilder("OK");
        for (TopicEnd e : host.snapshotEnds())
            sb.append('\n').append(e.topic()).append(' ').append(e.partition()).append(' ').append(e.end());
        return sb.toString();
    }

    private void acceptLeaderLocked(long term, String fromLeader, String addr) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            persistLocked();
        }
        if (role != Role.FOLLOWER) becomeFollowerLocked(term);
        if (!fromLeader.equals(leaderId)) {
            audit.record("LEADER_KNOWN", cfg.nodeId(), "leader", fromLeader, "term", String.valueOf(term));
        }
        leaderId = fromLeader;
        leaderAddr = addr;
        resetElectionTimerLocked();
    }

    // ================================================================== status and helpers

    public ClusterStatus status() {
        long now = System.currentTimeMillis();
        synchronized (lock) {
            List<ClusterStatus.PeerStatus> ps = new ArrayList<>();
            for (Link l : links.values())
                ps.add(new ClusterStatus.PeerStatus(l.peer.id(), l.peer.address(),
                        l.lastOk == 0 ? -1 : now - l.lastOk, l.inSync));
            return new ClusterStatus(true, cfg.nodeId(), role.name(), currentTerm, leaderId, leaderAddr, ps);
        }
    }

    public Role role() {
        synchronized (lock) { return role; }
    }

    public String leaderId() {
        synchronized (lock) { return leaderId; }
    }

    private boolean isLeaderInTerm(long term) {
        synchronized (lock) { return role == Role.LEADER && currentTerm == term; }
    }

    private int majority() { return (links.size() + 1) / 2 + 1; }

    //"How much data does this node have": total records over all partitions. Used to refuse votes to stale candidates.
    private long score() throws IOException {
        long s = 0;
        for (TopicEnd e : host.snapshotEnds()) s += e.end();
        return s;
    }

    private void resetElectionTimerLocked() {
        long base = cfg.electionTimeoutMs();
        electionDeadline = System.currentTimeMillis() + base + (long) (random.nextDouble() * base);
    }

    private static String key(String topic, int partition) { return topic + "-" + partition; }

    // ---- term and vote must survive a restart, otherwise a node could vote twice in one term ----

    private void loadState() {
        Path f = cfg.stateFile();
        if (f == null || !Files.exists(f)) return;
        try (FileInputStream in = new FileInputStream(f.toFile())) {
            Properties pr = new Properties();
            pr.load(in);
            currentTerm = Long.parseLong(pr.getProperty("term", "0"));
            votedFor = pr.getProperty("votedFor");
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("cannot read cluster state " + f + ": " + e.getMessage(), e);
        }
    }

    private void persistLocked() {
        Path f = cfg.stateFile();
        if (f == null) return;
        try {
            Path tmp = f.resolveSibling(f.getFileName() + ".tmp");
            Properties pr = new Properties();
            pr.setProperty("term", String.valueOf(currentTerm));
            if (votedFor != null) pr.setProperty("votedFor", votedFor);
            try (FileOutputStream out = new FileOutputStream(tmp.toFile())) {
                pr.store(out, "minikafka cluster state");
                out.flush();
                out.getFD().sync();
            }
            Files.move(tmp, f, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ================================================================== connection to one peer

    private final class Link {
        final Peer peer;
        final AtomicBoolean syncing = new AtomicBoolean();
        volatile long lastOk;
        volatile boolean inSync;
        private Socket socket;
        private DataInputStream in;
        private DataOutputStream out;

        Link(Peer peer) { this.peer = peer; }

        //One request, one reply. Requests on a link are serialised; any failure closes the socket so the next call reconnects.
        synchronized String call(String header, byte[] body) throws IOException {
            try {
                if (socket == null) {
                    Socket s = new Socket();
                    s.connect(new InetSocketAddress(peer.host(), peer.port()), 500);
                    s.setSoTimeout((int) Math.max(200, cfg.ackTimeoutMs()));
                    s.setTcpNoDelay(true);
                    socket = s;
                    in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                    out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                }
                byte[] h = header.getBytes(StandardCharsets.UTF_8);
                byte[] req;
                if (body == null) {
                    req = h;
                } else {
                    req = new byte[h.length + 1 + body.length];
                    System.arraycopy(h, 0, req, 0, h.length);
                    req[h.length] = '\n';
                    System.arraycopy(body, 0, req, h.length + 1, body.length);
                }
                Frames.send(out, req);
                return new String(Frames.read(in), StandardCharsets.UTF_8);
            } catch (IOException e) {
                closeQuietly();
                inSync = false;
                throw e;
            }
        }

        private void closeQuietly() {
            try {
                if (socket != null) socket.close();
            } catch (IOException ignored) {
                // nothing to do
            }
            socket = null;
        }

        synchronized void close() { closeQuietly(); }
    }
}
