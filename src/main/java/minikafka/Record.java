package minikafka;

/** A record read back from a log: its offset and payload bytes. */
public record Record(long offset, byte[] payload) {}
