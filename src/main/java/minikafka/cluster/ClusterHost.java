package minikafka.cluster;

import minikafka.Record;

import java.io.IOException;
import java.util.List;

//What ClusterNode needs from the local broker: raw access to the local logs, bypassing
//the leader check and the high watermark.
public interface ClusterHost {
    long endOffset(String topic, int partition) throws IOException;

    List<Record> readLocal(String topic, int partition, long from, int max) throws IOException;

    //Append only if the log is exactly at expectedOffset. Returns the offset, or -1 when it is not.
    long appendReplica(String topic, int partition, long expectedOffset, byte[] payload) throws IOException;

    void truncate(String topic, int partition, long toOffset) throws IOException;

    //End offset of every partition this node has (including the internal offsets log)
    List<TopicEnd> snapshotEnds() throws IOException;

    //This node just became leader (true) or stopped being leader (false)
    void onLeadershipChange(boolean leader);
}
