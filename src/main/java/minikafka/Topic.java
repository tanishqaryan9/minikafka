package minikafka;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

//A topic = N independent partition logs. Same key -> same partition -> per-key ordering
public class Topic implements Closeable {
    @Getter @Accessors(fluent = true)
    private final String name;
    private final AppendLog[] partitions;
    private final GroupCommitter[] committers;   // null when group commit is switched off

    //Plain topic: every append does its own fsync
    public Topic(Path dataDir, String name, int numPartitions) throws IOException {
        this(dataDir, name, numPartitions, 0, 0);
    }

    //maxBatch <= 0 disables group commit; otherwise each partition gets its own writer thread
    public Topic(Path dataDir, String name, int numPartitions, int maxBatch, int queueCapacity) throws IOException {
        this.name = name;
        Files.createDirectories(dataDir);
        partitions = new AppendLog[numPartitions];
        for (int i = 0; i < numPartitions; i++)
            partitions[i] = new AppendLog(dataDir.resolve(name + "-" + i + ".log"));
        if (maxBatch > 0) {
            committers = new GroupCommitter[numPartitions];
            for (int i = 0; i < numPartitions; i++)
                committers[i] = new GroupCommitter(partitions[i], maxBatch, queueCapacity, name + "-" + i);
        } else {
            committers = null;
        }
    }

    public int numPartitions() { return partitions.length; }
    public AppendLog partition(int i) { return partitions[i]; }

    public int partitionFor(String key) {
        return Math.floorMod(key.hashCode(), partitions.length);
    }

    //Durable append to one partition (through the group committer when enabled). Returns the offset.
    public long append(int partition, byte[] payload) throws IOException {
        GroupCommitter gc = committers == null ? null : committers[partition];
        return gc != null ? gc.append(payload) : partitions[partition].append(payload);
    }

    @Override
    public void close() throws IOException {
        if (committers != null) for (GroupCommitter gc : committers) gc.close();   // stop writers first
        for (AppendLog p : partitions) p.close();
    }
}
