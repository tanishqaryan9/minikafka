package minikafka.cluster;

import java.util.List;

public record ClusterStatus(boolean enabled, String nodeId, String role, long term,
                            String leaderId, String leaderAddress, List<PeerStatus> peers) {

    public record PeerStatus(String id, String address, long lastContactAgoMs, boolean inSync) {}

    public static ClusterStatus standalone(String nodeId) {
        return new ClusterStatus(false, nodeId, "STANDALONE", 0, nodeId, null, List.of());
    }
}
