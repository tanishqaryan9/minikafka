package minikafka.cluster;

//A client talked to a node that is not the leader. leaderAddress may be null (election in progress).
public class NotLeaderException extends RuntimeException {
    private final String leaderAddress;

    public NotLeaderException(String leaderAddress) {
        super("NOT_LEADER " + (leaderAddress == null ? "unknown" : leaderAddress));
        this.leaderAddress = leaderAddress;
    }

    public String leaderAddress() { return leaderAddress; }
}
