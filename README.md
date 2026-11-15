# MiniKafka (Java 21, Spring Boot, Maven)

A Kafka-style durable log, built from scratch, packaged as a Spring Boot application.
It covers all of the 7-week lab **plus** the stretch phase:

| Lab phase | What is in this project |
|---|---|
| Week 1-2: durable log, atomic offsets, crash recovery | `AppendLog`: `[len][crc32][payload]`, fsync before ack, torn-tail truncation, CRC check |
| Week 2: producer-consumer + group commit | `BoundedBuffer` feeds `GroupCommitter`: many producers share one fsync |
| Week 3: TCP server, thread pool | `Broker` + `Frames` (length-prefixed protocol) |
| Week 4: sparse index | one bookmark per 1000 records; `Bench` shows index vs full scan |
| Week 5: topics, partitions | `Topic`, key hashing with `floorMod` |
| Week 6: consumers, committed offsets, groups | `GroupCoordinator` + `OffsetStore` (durable commits), at-least-once delivery |
| Week 7: benchmarks | `minikafka.bench.Bench` (throughput, p50/p95/p99, fsync batching) |
| Stretch: replication + failover | `ClusterNode`: majority-acknowledged writes, term/vote elections, log repair |
| Requested extra: audit trail | `AuditLog`: **separate file** of what was added, what was consumed, and by whom |

## Run

    mvn test                          # 90 tests (see "Testing")
    mvn spring-boot:run               # TCP broker :9092, REST + actuator :8080, data in ./data, logs in ./logs
    ./run.sh demo                     # two producers + two consumers; then read logs/audit.log
    ./run.sh bench                    # write/read benchmark
    ./run.sh cluster start            # 3 nodes on this machine (tcp 9092-9094, rest 8081-8083)

Postman: import `postman/MiniKafka.postman_collection.json` (29 requests with tests, run the folders in order).

## The audit log: what was added, what was consumed, by whom

Written to its **own file**, `logs/audit.log` (the application log is `logs/minikafka.log`). One line per event:

    time [node] EVENT actor=<protocol>:<client id>@<remote address> key=value ...

Real output from `./run.sh demo`:

    PRODUCE actor=tcp:order-service@127.0.0.1:48082 topic=demo-orders partition=2 offset=0 key=customer-1 bytes=8 preview="order #1"
    JOIN    actor=tcp:billing-c2@127.0.0.1:55946 group=billing topic=demo-orders consumer=c2 newMember=true
    ASSIGN  actor=tcp:billing-c2@127.0.0.1:55946 group=billing topic=demo-orders generation=2 reason="join of c2" assignment="c1=[0,1];c2=[2,3]"
    CONSUME actor=tcp:billing-c1@127.0.0.1:55944 group=billing consumer=c1 topic=demo-orders partition=0 offset=0 bytes=8 preview="order #3"
    COMMIT  actor=tcp:billing-c1@127.0.0.1:55944 group=billing consumer=c1 topic=demo-orders offsets="{0=2,1=2}"
    CONSUME actor=tcp:audit-dashboard@127.0.0.1:55950 topic=demo-orders partition=0 offset=0 bytes=8 preview="order #3"

**Who is "actor"?** Over TCP, `new Client(host, port, "order-service")` sends `HELLO order-service`; without an id the
actor is `tcp:anonymous@...`. Over REST, send the header `X-Client-Id: order-service` (group endpoints fall back to the
consumer name). The remote address is always recorded, so even an anonymous caller is traceable.

| Event | Meaning |
|---|---|
| `PRODUCE` / `PRODUCE_FAILED` | a record was added (topic, partition, offset, key, size, preview) / could not be stored or replicated |
| `CONSUME` | a record was handed to a reader: a plain `FETCH` (no group) or a group `POLL` (with group + consumer) |
| `JOIN` `LEAVE` `EXPIRE` `ASSIGN` | group membership; `ASSIGN` shows which consumer owns which partitions after each rebalance |
| `COMMIT` | a group made its progress durable |
| `CONNECT` `HELLO` `DISCONNECT` | client connections (`DISCONNECT` shows how many requests it made) |
| `NODE_START` `NODE_STOP` | broker lifecycle |
| `ELECTION_START` `VOTE_GRANTED` `LEADER_ELECTED` `ROLE_CHANGE` `QUORUM_LOST` `REPLICATED` `TRUNCATE` `FOLLOWER_SYNCED` | cluster activity (each node writes its own audit file) |

Settings (`minikafka.audit.*`): `file`, `max-file-size-mb` + `max-backups` (rolls to `audit.log.1`, `.2`, ...),
`payload-preview` (characters of content shown; **set 0 if records are sensitive**), `consume-per-record`
(false = one summary line per batch), `enabled`. Values containing spaces or `=` are quoted, and control
characters are replaced, so a record's content can never forge a second log line.
Query it without opening the file: `GET /api/audit?type=CONSUME&actor=billing&topic=orders&group=billing&limit=50`.

## REST API

| Method and path | Purpose |
|---|---|
| `POST /api/topics/{topic}/messages?key=` | produce (body = text) -> `{"partition":0,"offset":7}` |
| `GET /api/topics/{topic}/partitions/{p}/messages/{offset}` | one record, 404 if not written yet |
| `GET /api/topics/{topic}/partitions/{p}/messages?from=&max=` | a batch |
| `GET /api/topics`, `GET /api/topics/{topic}` | topics and end offsets |
| `POST /api/groups/{g}/topics/{t}/join?consumer=` | join; returns generation and owned partitions |
| `GET  /api/groups/{g}/topics/{t}/poll?consumer=&max=` | next records of *my* partitions |
| `POST /api/groups/{g}/topics/{t}/commit?consumer=[&partition=&offset=]` | commit current positions, or one partition |
| `POST /api/groups/{g}/topics/{t}/heartbeat?consumer=` , `.../leave?consumer=` | keep alive / leave |
| `GET /api/groups/{g}` | members, owners, committed offset, position, lag per partition |
| `GET /api/audit`, `GET /api/audit/info` | audit trail / where the file is |
| `GET /api/cluster` | role (`STANDALONE`, `FOLLOWER`, `CANDIDATE`, `LEADER`), term, leader, peers |

Errors: 400 bad name/offset/partition, 404 no record yet, 409 not a member of the group, 503 not the leader
(`X-Leader-Address` header says who is) or not replicated to a majority.

## TCP protocol (what `Client` speaks)

    HELLO <clientId>                          -> OK
    PRODUCE <topic> <key>\n<payload>          -> OK <partition> <offset>
    FETCH <topic> <partition> <offset>        -> OK <payload> | EMPTY
    FETCHB <topic> <partition> <offset> <max> -> OK 0 <count>\n + records
    JOIN <group> <topic> <consumer>           -> OK <generation> <p1,p2,...|->
    POLL <group> <topic> <consumer> <max>     -> OK <generation> <count>\n + records
    COMMIT <group> <topic> <consumer> [p:o,..]-> OK <p:o,...>
    BEAT / LEAVE <group> <topic> <consumer>   -> OK
    records = repeated: int partition | long offset | int length | payload

Every request and reply is a frame: `[4-byte length][bytes]`. Names may not contain spaces over TCP.

## Consumer groups: how they behave

* Each partition has exactly one owner inside a group (range assignment, members sorted by name). Joining or leaving
  triggers a rebalance and bumps the *generation*. A consumer silent for `group.session-timeout-ms` is expired.
* **At-least-once.** The read position lives in memory; only `commit` makes it durable (appended to the internal
  log `__consumer_offsets.log`, replayed on startup). After a rebalance or restart each partition resumes from its last
  *committed* offset, so records polled but not committed are delivered again. Commit after you have processed.
* Reserved: topic names starting with `__`. A consumer can only commit partitions it owns.

## Replication and failover (cluster mode)

Set `minikafka.cluster.enabled=true` and list the other nodes, e.g. `peers: ["node-2=localhost:9093","node-3=localhost:9094"]`
(all nodes must use the same `partitions`). `./run.sh cluster start` does this for three local nodes.

* One **leader** serves all client reads and writes; followers answer clients with `NOT_LEADER <address>`.
* **Write:** leader appends, sends the record to every follower, and replies only when a **majority** (itself included)
  has it. Consumers only see records below the *high watermark* (those committed on a majority).
* **Election:** a follower silent for the (randomised) election timeout becomes a candidate: term+1, votes for itself,
  asks the others. One vote per node per term (persisted in `cluster-state.properties`), and only for a candidate whose
  logs are at least as long. A majority of votes wins. A leader that cannot reach a majority steps down.
* **Repair:** a new leader compares every follower's logs with its own, pushes missing records, and overwrites records
  where a follower disagrees (written by an old leader that never reached a majority). Committed offsets are replicated
  like any other log, so group progress survives a failover; group *membership* does not (consumers re-join).

This is "Raft-lite": no per-record terms (logs are compared by length and by the CRC of the preceding record), no
membership changes, no snapshots. A record that failed to reach a majority is reported as failed but may still be
committed later if it is in the new leader's log, so a client that retries can create a duplicate (at-least-once).

## Benchmarks (`./run.sh bench`)

Measured in the build sandbox (1 CPU, virtual disk; `appends=3000 threads=16 payload=100`). **Compare rows with each
other, not with your machine.** An fsync costs far more on many real disks, which makes group commit matter more.

    mode                                ops/sec   mean ms    p50 ms    p95 ms    p99 ms
    1 thread,  fsync per record            7292     0.137     0.114     0.209     0.634
    16 threads, fsync per record           6754     2.328     2.190     4.162     7.263
    16 threads, group commit              26886     0.585     0.216     2.045     7.938   201 fsyncs, avg batch 14.9
    1 thread,  group commit                6074     0.164     0.124     0.222     1.130   (no one to share an fsync with)

    read one record, log of 100000        mean us    p99 us   speedup
    offset 1500   scan from byte 0         1008.5    4772.3
                  sparse index              421.0    4324.5      2.4x
    offset 50500  scan from byte 0        17582.4   22971.0
                  sparse index              280.9    4204.1     62.6x
    offset 99999  scan from byte 0        34288.2   47665.9
                  sparse index              325.5     423.6    105.3x
    readBatch(500) sequential: 688201 records/sec

Reading it: threads alone do not help (the lock serialises the fsyncs); group commit does, because 16 waiting
producers share one fsync (avg batch 14.9). A single producer gains nothing. The index cost is bounded (at most 999
headers scanned) while a full scan grows with the log.

## Configuration (`src/main/resources/application.yml`)

Every key can be overridden: `--minikafka.port=9093`, or the environment variable `MINIKAFKA_PORT=9093`.
Keys: `port`, `data-dir`, `partitions`, `worker-threads`, `node-id`, `audit.*`, `group-commit.{enabled,max-batch,queue-capacity}`,
`group.session-timeout-ms`, `cluster.{enabled,peers,advertise,heartbeat-ms,election-timeout-ms,ack-timeout-ms}`.

## Testing

90 tests: storage and recovery (`AppendLogTest`, `AppendLogBatchTest`), blocking queue, group commit (including a
test that counts fsync rounds), audit file format/rotation/filters, offset store, group coordinator (assignment,
redelivery, commit rules, expiry, fairness), real brokers over TCP (`BrokerTcpTest`), a real 3-node cluster
(`ClusterFailoverTest`: election, replication, follower catch-up, failover without loss, a diverged old leader being
repaired, isolated leader stepping down, consumer progress surviving failover), and the full Spring application over
HTTP (`BrokerIntegrationTest`, uses only `java.net.http` so it does not depend on Spring test-support packages that
moved in Boot 4).

## Layout

    minikafka/            AppendLog BoundedBuffer GroupCommitter Frames Record Topic Broker Client
    minikafka/audit       AuditLog
    minikafka/group       GroupCoordinator OffsetStore
    minikafka/cluster     ClusterNode Replicator ClusterHost NotLeaderException ReplicationException ...
    minikafka/config      BrokerProperties AuditConfig
    minikafka/web         Topic/Group/Audit/Cluster controllers, ApiExceptionHandler
    minikafka/bench       Bench          minikafka/cli   Demo

## Known limits

Single log file per partition (no segments or retention); one thread per open TCP connection (`worker-threads`
bounds concurrent clients; an NIO selector would lift that); `__consumer_offsets` is not compacted; the audit
file records payload previews, so protect it like the data itself; no authentication or TLS.
