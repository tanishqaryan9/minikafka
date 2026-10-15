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

    public Topic(Path dataDir, String name, int numPartitions) throws IOException {
        this.name = name;
        Files.createDirectories(dataDir);
        partitions = new AppendLog[numPartitions];
        for (int i = 0; i < numPartitions; i++)
            partitions[i] = new AppendLog(dataDir.resolve(name + "-" + i + ".log"));
    }

    public int numPartitions() { return partitions.length; }
    public AppendLog partition(int i) { return partitions[i]; }

    public int partitionFor(String key) {
        return Math.floorMod(key.hashCode(), partitions.length);
    }

    @Override
    public void close() throws IOException {
        for (AppendLog p : partitions) p.close();
    }
}
