package minikafka.group;

import minikafka.AppendLog;
import minikafka.Record;
import minikafka.cluster.Replicator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

//Where consumer groups remember how far they have read ("committed offsets").
//Same idea as Kafka's internal __consumer_offsets topic: every commit is APPENDED to a durable log
//(an AppendLog, so it gets the CRC + fsync + crash recovery for free) and the in-memory map is just
//the latest value per (group, topic, partition), rebuilt by replaying the log on startup.
//
//Record text:  group \t topic \t partition \t offset
//The stored offset is the NEXT offset the group wants to read (last processed + 1).
public final class OffsetStore implements Closeable {
    public static final String TOPIC = "__consumer_offsets";   // name used when replicating this log
    public static final String FILE = TOPIC + ".log";

    private final AppendLog log;
    private final Replicator replicator;
    private final Map<String, Long> committed = new HashMap<>();

    public OffsetStore(Path file, Replicator replicator) throws IOException {
        this.log = new AppendLog(file);
        this.replicator = replicator;
        rebuild();
    }

    //Durably commit offsets for one group/topic: all partitions go out in ONE write + ONE fsync.
    public synchronized void commit(String group, String topic, Map<Integer, Long> offsets) throws IOException {
        if (offsets.isEmpty()) return;
        replicator.requireLeader();
        List<byte[]> payloads = new java.util.ArrayList<>();
        for (Map.Entry<Integer, Long> e : new TreeMap<>(offsets).entrySet())
            payloads.add(encode(group, topic, e.getKey(), e.getValue()));
        long base = log.appendAll(payloads);
        for (byte[] p : payloads) apply(p);
        for (int i = 0; i < payloads.size(); i++) replicator.replicate(TOPIC, 0, base + i, payloads.get(i));
    }

    //Committed offset, or -1 if this group never committed for that partition
    public synchronized long get(String group, String topic, int partition) {
        return committed.getOrDefault(key(group, topic, partition), -1L);
    }

    // ---- used when this node is a follower and the leader replicates commits to it ----

    public synchronized long appendReplica(long expectedOffset, byte[] payload) throws IOException {
        long off = log.appendIfNext(expectedOffset, payload);
        if (off >= 0) apply(payload);
        return off;
    }

    public synchronized void truncateTo(long offset) throws IOException {
        log.truncateTo(offset);
        rebuild();
    }

    public long endOffset() { return log.nextOffset(); }

    public List<Record> read(long from, int max) throws IOException { return log.readBatch(from, max); }

    // ---- internals ----

    private void rebuild() throws IOException {
        committed.clear();
        long from = 0;
        while (true) {
            List<Record> batch = log.readBatch(from, 500);
            if (batch.isEmpty()) break;
            for (Record r : batch) apply(r.payload());
            from = batch.get(batch.size() - 1).offset() + 1;
        }
    }

    private void apply(byte[] payload) {
        String[] f = new String(payload, StandardCharsets.UTF_8).split("\t");
        if (f.length != 4) return;                       // ignore anything we do not understand
        committed.put(key(f[0], f[1], Integer.parseInt(f[2])), Long.parseLong(f[3]));
    }

    private static byte[] encode(String group, String topic, int partition, long offset) {
        return (group + "\t" + topic + "\t" + partition + "\t" + offset).getBytes(StandardCharsets.UTF_8);
    }

    private static String key(String group, String topic, int partition) {
        return group + "\t" + topic + "\t" + partition;
    }

    @Override
    public synchronized void close() throws IOException { log.close(); }
}
