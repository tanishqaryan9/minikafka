# MiniKafka (Java 21, Spring Boot 3.5, Maven)

A from-scratch, Kafka-style durable log, now packaged as a Spring Boot application.
Lombok is used for boilerplate; the storage engine itself still has no extra dependencies.

## Run
    mvn test                 # JUnit 5: storage, recovery, concurrency, TCP + REST end-to-end
    mvn spring-boot:run      # TCP broker on :9092, REST API + actuator on :8080, data in ./data
    mvn package && java -jar target/minikafka-0.1.0-SNAPSHOT.jar --minikafka.port=9093

(`./run.sh test` / `./run.sh broker` are thin wrappers.)

## Configuration (`src/main/resources/application.yml`)
| key | default | meaning |
|---|---|---|
| `minikafka.port` | 9092 | raw TCP broker port (0 = random) |
| `minikafka.data-dir` | data | partition log directory |
| `minikafka.partitions` | 4 | partitions per new topic |
| `minikafka.worker-threads` | 16 | connection handler pool size |
| `server.port` | 8080 | REST / actuator port |

## REST API
    curl -X POST -H 'Content-Type: text/plain' -d 'deposit' 'localhost:8080/api/topics/orders/messages?key=alice'
    # -> {"partition":1,"offset":0}
    curl localhost:8080/api/topics/orders/partitions/1/messages/0     # -> deposit (404 if not written yet)
    curl localhost:8080/actuator/health

## Implemented
- `AppendLog`  — record = [len][crc32][payload]; fsync before ack; atomic offsets under a lock;
                 sparse index (every 1000th offset); startup recovery truncates a torn/corrupt tail
- `BoundedBuffer` — blocking ring buffer (ReentrantLock + Condition)
- `Frames`     — length-prefix framing over TCP
- `Topic`      — N partition logs; floorMod key hashing keeps per-key order
- `Broker`     — Spring `SmartLifecycle` bean: thread-pool TCP server (PRODUCE / FETCH) + `produce()`/`fetch()` API
- `TopicController` — REST front-end over the same broker
- `Client`     — minimal blocking TCP client

## Not yet implemented
Consumer groups & committed offsets, NIO selector server, group-commit fsync + benchmarks,
replication / leader election, log segments / retention.
