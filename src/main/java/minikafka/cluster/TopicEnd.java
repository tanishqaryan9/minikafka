package minikafka.cluster;

//End offset (next offset to be written) of one partition on one node
public record TopicEnd(String topic, int partition, long end) {}
