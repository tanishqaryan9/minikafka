package minikafka.group;

import minikafka.Record;
import minikafka.audit.AuditLog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

//Consumer groups: several consumers share one topic, and each PARTITION is read by exactly ONE of them.
//
//  join   -> consumer becomes a member; partitions are re-split among members (a "rebalance")
//  poll   -> read the next records of MY partitions, starting at the group's current position
//  commit -> durably remember the position (OffsetStore); survives restarts
//  leave / session timeout -> member removed, partitions re-split among the rest
//
//Semantics are Kafka's "at least once": the position lives in memory, only commit() makes it durable.
//After a rebalance or a restart every partition restarts from its last COMMITTED offset, so records that
//were polled but never committed are delivered again.
//
//Every action is written to the audit log (who joined, who got which partitions, who consumed what).
public final class GroupCoordinator {
    //What the coordinator needs from the broker. Reads must already be capped at the high watermark.
    public interface Partitions {
        int partitionCount(String topic) throws IOException;                 // also creates the topic if new
        List<Record> read(String topic, int partition, long from, int max) throws IOException;
        long endOffset(String topic, int partition) throws IOException;      // high watermark
    }

    public record JoinResult(int generation, List<Integer> partitions) {}
    public record PolledRecord(int partition, long offset, byte[] payload) {}
    public record PollResult(int generation, List<PolledRecord> records) {}
    public record MemberInfo(String consumer, List<Integer> partitions, long idleMs) {}
    public record PartitionLag(int partition, String owner, long committed, long position, long end, long lag) {}
    public record TopicInfo(String topic, int generation, List<MemberInfo> members, List<PartitionLag> partitions) {}
    public record GroupInfo(String group, List<TopicInfo> topics) {}

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9._-]{1,100}");
    static final int MAX_POLL_BYTES = 8 * 1024 * 1024;     // keep one reply well under the 16 MB frame limit

    //State of one (group, topic) pair
    private static final class Unit {
        final String group, topic;
        final int partitions;
        int generation;
        int roundRobin;
        final TreeMap<String, Long> members = new TreeMap<>();      // consumer -> last time we heard from it
        final Map<Integer, String> owner = new TreeMap<>();         // partition -> consumer
        final Map<Integer, Long> position = new HashMap<>();        // partition -> next offset to deliver

        Unit(String group, String topic, int partitions) {
            this.group = group;
            this.topic = topic;
            this.partitions = partitions;
        }

        List<Integer> assigned(String consumer) {
            List<Integer> mine = new ArrayList<>();
            for (Map.Entry<Integer, String> e : owner.entrySet()) if (e.getValue().equals(consumer)) mine.add(e.getKey());
            return mine;
        }
    }

    private final Partitions view;
    private final OffsetStore offsets;
    private final AuditLog audit;
    private final long sessionTimeoutMs;
    private final LongSupplier clock;
    private final ConcurrentHashMap<String, Unit> units = new ConcurrentHashMap<>();

    public GroupCoordinator(Partitions view, OffsetStore offsets, AuditLog audit, long sessionTimeoutMs, LongSupplier clock) {
        this.view = view;
        this.offsets = offsets;
        this.audit = audit;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.clock = clock;
    }

    // ------------------------------------------------------------------ membership

    //Idempotent: a consumer that is already a member just gets its current assignment back
    public JoinResult join(String group, String topic, String consumer, String actor) throws IOException {
        checkName("group", group);
        checkName("consumer", consumer);
        int n = view.partitionCount(topic);
        Unit u = units.computeIfAbsent(key(group, topic), k -> new Unit(group, topic, n));
        synchronized (u) {
            long now = clock.getAsLong();
            sweep(u, now);
            boolean isNew = u.members.put(consumer, now) == null;
            audit.record("JOIN", actor, "group", group, "topic", topic, "consumer", consumer,
                    "newMember", String.valueOf(isNew));
            if (isNew) rebalance(u, actor, "join of " + consumer);
            return new JoinResult(u.generation, u.assigned(consumer));
        }
    }

    public void leave(String group, String topic, String consumer, String actor) {
        Unit u = existing(group, topic);
        synchronized (u) {
            sweep(u, clock.getAsLong());
            if (u.members.remove(consumer) == null) throw notMember(group, consumer);
            audit.record("LEAVE", actor, "group", group, "topic", topic, "consumer", consumer);
            rebalance(u, actor, "leave of " + consumer);
        }
    }

    //Keeps the session alive; returns the generation so the consumer can notice that a rebalance happened
    public int heartbeat(String group, String topic, String consumer) {
        Unit u = existing(group, topic);
        synchronized (u) {
            long now = clock.getAsLong();
            sweep(u, now);
            requireMember(u, consumer);
            u.members.put(consumer, now);
            return u.generation;
        }
    }

    // ------------------------------------------------------------------ consuming

    public PollResult poll(String group, String topic, String consumer, int max, String actor) throws IOException {
        Unit u = existing(group, topic);
        synchronized (u) {
            long now = clock.getAsLong();
            sweep(u, now);
            requireMember(u, consumer);
            u.members.put(consumer, now);

            List<PolledRecord> out = new ArrayList<>();
            List<Integer> mine = u.assigned(consumer);
            int budget = Math.max(1, max);
            long bytes = 0;
            int start = mine.isEmpty() ? 0 : Math.floorMod(u.roundRobin++, mine.size());   // do not always favour partition 0
            for (int i = 0; i < mine.size() && budget > 0 && bytes < MAX_POLL_BYTES; i++) {
                int p = mine.get((start + i) % mine.size());
                List<Record> recs = view.read(topic, p, u.position.get(p), budget);
                List<Record> taken = new ArrayList<>();
                for (Record r : recs) {
                    if (!taken.isEmpty() && bytes + r.payload().length > MAX_POLL_BYTES) break;
                    taken.add(r);
                    bytes += r.payload().length;
                }
                if (taken.isEmpty()) continue;
                for (Record r : taken) out.add(new PolledRecord(p, r.offset(), r.payload()));
                u.position.put(p, taken.get(taken.size() - 1).offset() + 1);
                budget -= taken.size();
                audit.consumed(actor, group, consumer, topic, p, taken);
            }
            return new PollResult(u.generation, out);
        }
    }

    //Commit explicit offsets (partition -> next offset to read), or, when 'explicit' is null, the current position
    //of every partition this consumer owns. A consumer can only commit partitions it owns.
    public Map<Integer, Long> commit(String group, String topic, String consumer,
                                     Map<Integer, Long> explicit, String actor) throws IOException {
        Unit u = existing(group, topic);
        synchronized (u) {
            long now = clock.getAsLong();
            sweep(u, now);
            requireMember(u, consumer);
            u.members.put(consumer, now);

            Map<Integer, Long> toCommit = new TreeMap<>();
            if (explicit == null) {
                for (int p : u.assigned(consumer)) toCommit.put(p, u.position.get(p));
            } else {
                for (Map.Entry<Integer, Long> e : explicit.entrySet()) {
                    int p = e.getKey();
                    long off = e.getValue();
                    if (!consumer.equals(u.owner.get(p)))
                        throw new IllegalStateException("partition " + p + " is not assigned to consumer " + consumer);
                    long end = view.endOffset(topic, p);
                    if (off < 0 || off > end)
                        throw new IllegalArgumentException("offset " + off + " out of range 0.." + end + " for partition " + p);
                    toCommit.put(p, off);
                }
            }
            if (toCommit.isEmpty()) return toCommit;
            offsets.commit(group, topic, toCommit);
            audit.record("COMMIT", actor, "group", group, "consumer", consumer, "topic", topic,
                    "offsets", toCommit.toString().replace(" ", ""));
            return toCommit;
        }
    }

    // ------------------------------------------------------------------ inspection

    public GroupInfo describe(String group) throws IOException {
        List<TopicInfo> topics = new ArrayList<>();
        for (Unit u : units.values()) {
            if (!u.group.equals(group)) continue;
            synchronized (u) {
                long now = clock.getAsLong();
                sweep(u, now);
                List<MemberInfo> members = new ArrayList<>();
                for (Map.Entry<String, Long> m : u.members.entrySet())
                    members.add(new MemberInfo(m.getKey(), u.assigned(m.getKey()), now - m.getValue()));
                List<PartitionLag> parts = new ArrayList<>();
                for (int p = 0; p < u.partitions; p++) {
                    long committed = offsets.get(group, u.topic, p);
                    long position = u.position.getOrDefault(p, Math.max(committed, 0));
                    long end = view.endOffset(u.topic, p);
                    parts.add(new PartitionLag(p, u.owner.get(p), committed, position, end,
                            Math.max(0, end - Math.max(committed, 0))));
                }
                topics.add(new TopicInfo(u.topic, u.generation, members, parts));
            }
        }
        topics.sort((a, b) -> a.topic().compareTo(b.topic()));
        return new GroupInfo(group, topics);
    }

    //Forget all membership and positions (committed offsets stay). Called when leadership changes hands.
    public void reset() {
        units.clear();
    }

    public List<String> groups() {
        return units.values().stream().map(u -> u.group).distinct().sorted().toList();
    }

    // ------------------------------------------------------------------ internals

    //Range assignment: members sorted by name, each gets a contiguous block of partitions
    private void rebalance(Unit u, String actor, String reason) {
        u.generation++;
        u.owner.clear();
        u.position.clear();
        List<String> ms = new ArrayList<>(u.members.keySet());
        if (!ms.isEmpty()) {
            int per = u.partitions / ms.size(), extra = u.partitions % ms.size(), next = 0;
            for (int i = 0; i < ms.size(); i++) {
                int count = per + (i < extra ? 1 : 0);
                for (int k = 0; k < count; k++) u.owner.put(next++, ms.get(i));
            }
            for (int p = 0; p < u.partitions; p++) {          // restart from the last committed offset
                long c = offsets.get(u.group, u.topic, p);
                u.position.put(p, c < 0 ? 0 : c);
            }
        }
        StringBuilder sb = new StringBuilder();
        for (String m : ms) {
            if (sb.length() > 0) sb.append(';');
            sb.append(m).append("=").append(u.assigned(m).toString().replace(" ", ""));
        }
        audit.record("ASSIGN", actor, "group", u.group, "topic", u.topic,
                "generation", String.valueOf(u.generation), "reason", reason,
                "assignment", sb.length() == 0 ? "none" : sb.toString());
    }

    //Drop members we have not heard from within the session timeout
    private void sweep(Unit u, long now) {
        List<String> dead = new ArrayList<>();
        for (Map.Entry<String, Long> m : u.members.entrySet())
            if (now - m.getValue() > sessionTimeoutMs) dead.add(m.getKey());
        if (dead.isEmpty()) return;
        for (String c : dead) {
            u.members.remove(c);
            audit.record("EXPIRE", "coordinator", "group", u.group, "topic", u.topic, "consumer", c,
                    "reason", "no heartbeat/poll for " + sessionTimeoutMs + "ms");
        }
        rebalance(u, "coordinator", "session timeout of " + dead);
    }

    private Unit existing(String group, String topic) {
        Unit u = units.get(key(group, topic));
        if (u == null) throw new IllegalStateException("group " + group + " has no members for topic " + topic + "; join first");
        return u;
    }

    private void requireMember(Unit u, String consumer) {
        if (!u.members.containsKey(consumer)) throw notMember(u.group, consumer);
    }

    private static IllegalStateException notMember(String group, String consumer) {
        return new IllegalStateException("consumer " + consumer + " is not a member of group " + group
                + " (never joined, left, or its session expired); join again");
    }

    private static void checkName(String what, String v) {
        if (v == null || !NAME.matcher(v).matches())
            throw new IllegalArgumentException("invalid " + what + " name (use letters, digits, . _ -)");
    }

    private static String key(String group, String topic) { return group + "/" + topic; }
}
