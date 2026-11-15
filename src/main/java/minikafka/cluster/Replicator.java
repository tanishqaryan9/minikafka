package minikafka.cluster;

import java.io.IOException;

//What the write path needs from the cluster layer. Standalone mode plugs in STANDALONE (does nothing);
//cluster mode plugs in ClusterNode.
public interface Replicator {
    boolean isClustered();

    //Throws NotLeaderException if this node may not accept client reads/writes right now
    void requireLeader();

    //Called AFTER the record is in the local log. Blocks until a majority of nodes have it
    //(or throws ReplicationException).
    void replicate(String topic, int partition, long offset, byte[] payload) throws IOException;

    //Records below this offset are committed (on a majority) and may be shown to consumers
    long highWatermark(String topic, int partition, long localEnd);

    Replicator STANDALONE = new Replicator() {
        @Override public boolean isClustered() { return false; }
        @Override public void requireLeader() { }
        @Override public void replicate(String topic, int partition, long offset, byte[] payload) { }
        @Override public long highWatermark(String topic, int partition, long localEnd) { return localEnd; }
    };
}
