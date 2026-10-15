package minikafka;

//Where a produced record landed
public record ProduceResult(int partition, long offset) {}
