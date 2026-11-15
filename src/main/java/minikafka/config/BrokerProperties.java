package minikafka.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;


@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "minikafka")
public class BrokerProperties {
    //TCP port of the framed-protocol broker. 0 picks a free port
    @Min(0) @Max(65535)
    private int port = 9092;

    @NotNull
    private Path dataDir = Path.of("data");

    //Partitions created for every new topic
    @Min(1)
    private int partitions = 4;

    //Size of the connection-handler thread pool
    @Min(1)
    private int workerThreads = 16;

    //Name of this node. Appears on every audit line, and is the node's identity inside a cluster
    @NotBlank
    private String nodeId = "node-1";

    @Valid private Audit audit = new Audit();
    @Valid private GroupCommit groupCommit = new GroupCommit();
    @Valid private Group group = new Group();
    @Valid private Cluster cluster = new Cluster();

    //Separate audit file: what was added, what was consumed, and by whom
    @Getter @Setter
    public static class Audit {
        private boolean enabled = true;
        private Path file = Path.of("logs", "audit.log");
        //Roll the file over when it reaches this size
        @Min(1) private int maxFileSizeMb = 10;
        //How many rolled files to keep (audit.log.1, audit.log.2, ...)
        @Min(0) private int maxBackups = 3;
        //Characters of each payload shown in the log (0 = never log payload content)
        @Min(0) private int payloadPreview = 40;
        //true: one CONSUME line per record. false: one line per batch
        private boolean consumePerRecord = true;
        //How many recent events are kept in memory for GET /api/audit
        @Min(1) private int memoryEvents = 2000;
    }

    //Several producers share one fsync (see GroupCommitter)
    @Getter @Setter
    public static class GroupCommit {
        private boolean enabled = true;
        //Most records written by one fsync
        @Min(1) private int maxBatch = 128;
        //Records that may wait for the disk writer before producers are slowed down
        @Min(1) private int queueCapacity = 1024;
    }

    @Getter @Setter
    public static class Group {
        //A consumer that neither polls, commits nor sends a heartbeat for this long is removed from its group
        @Min(1) private long sessionTimeoutMs = 30000;
    }

    //Replication and leader election. Disabled = a normal single broker.
    @Getter @Setter
    public static class Cluster {
        private boolean enabled = false;
        //The OTHER nodes, as id=host:tcpPort, for example node-2=localhost:9093
        private List<String> peers = new ArrayList<>();
        //How this node tells others to reach it. Empty = localhost:<tcp port>
        private String advertise = "";
        @Min(10) private long heartbeatMs = 500;
        //A follower starts an election after silence of this long (randomised up to twice as long)
        @Min(50) private long electionTimeoutMs = 3000;
        //How long a produce waits for a majority of nodes before failing
        @Min(50) private long ackTimeoutMs = 3000;
    }
}
