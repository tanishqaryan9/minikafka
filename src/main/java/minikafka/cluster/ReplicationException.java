package minikafka.cluster;

import java.io.IOException;

//The record is in the leader's log but a majority of nodes did not confirm it in time.
public class ReplicationException extends IOException {
    public ReplicationException(String message) { super(message); }
}
