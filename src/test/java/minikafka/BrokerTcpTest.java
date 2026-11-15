package minikafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static minikafka.TestSupport.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

//A real broker over real TCP sockets, driven through the Client
class BrokerTcpTest {
    @TempDir Path tmp;
    TestSupport.Node node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) node.stop();
    }

    TestSupport.Node start() throws Exception {
        node = TestSupport.start(tmp, "node-1", null);
        return node;
    }

    List<String> auditLines() throws IOException { return node.auditLines(); }

    // ------------------------------------------------------------------ produce / fetch, and the audit trail

    @Test
    void produceAndFetchRoundTripWithNamedClients() throws Exception {
        start();
        try (var producer = new Client("localhost", node.port(), "order-service");
             var reader = new Client("localhost", node.port(), "audit-dashboard")) {
            String r1 = producer.produce("orders", "alice", "deposit");
            String r2 = producer.produce("orders", "alice", "withdraw");
            String[] a = r1.split(":"), b = r2.split(":");
            assertThat(a[0]).isEqualTo(b[0]);                                       // same key, same partition
            assertThat(Long.parseLong(b[1])).isEqualTo(Long.parseLong(a[1]) + 1);
            assertThat(reader.fetch("orders", Integer.parseInt(a[0]), Long.parseLong(a[1]))).isEqualTo("deposit");
            assertThat(reader.fetch("orders", Integer.parseInt(b[0]), Long.parseLong(b[1]) + 1)).isNull();
        }
        List<String> lines = auditLines();
        assertThat(lines).anyMatch(l -> l.contains(" PRODUCE actor=tcp:order-service@127.0.0.1:") && l.contains("key=alice") && l.contains("preview=deposit"));
        assertThat(lines).anyMatch(l -> l.contains(" PRODUCE actor=tcp:order-service@127.0.0.1:") && l.contains("preview=withdraw"));
        assertThat(lines).anyMatch(l -> l.contains(" CONSUME actor=tcp:audit-dashboard@127.0.0.1:") && l.contains("preview=deposit"));
        // the empty fetch consumed nothing, so it is not in the trail; and the dashboard never produced anything
        assertThat(lines).noneMatch(l -> l.contains(" PRODUCE actor=tcp:audit-dashboard"));
        assertThat(lines).noneMatch(l -> l.contains(" CONSUME") && l.contains("order-service"));
    }

    @Test
    void anonymousClientsAreStillTraced() throws Exception {
        start();
        try (var c = new Client("localhost", node.port())) {
            c.produce("orders", "k", "v");
        }
        assertThat(auditLines()).anyMatch(l -> l.contains(" PRODUCE actor=tcp:anonymous@127.0.0.1:"));
    }

    @Test
    void connectAndDisconnectAreRecordedWithTheNumberOfRequests() throws Exception {
        start();
        try (var c = new Client("localhost", node.port(), "svc-a")) {
            c.produce("orders", "k", "v");
            c.produce("orders", "k", "v2");
        }
        await("DISCONNECT line", 3000, () -> {
            try { return auditLines().stream().anyMatch(l -> l.contains(" DISCONNECT actor=tcp:svc-a@")); }
            catch (IOException e) { return false; }
        });
        String line = auditLines().stream().filter(l -> l.contains(" DISCONNECT ")).findFirst().orElseThrow();
        assertThat(line).contains("requests=3");                                    // HELLO + two produces
        assertThat(auditLines()).anyMatch(l -> l.contains(" CONNECT actor="));
    }

    @Test
    void fetchBatchReturnsRecordsInOrder() throws Exception {
        start();
        try (var c = new Client("localhost", node.port(), "p")) {
            for (int i = 0; i < 10; i++) c.produce("events", "same-key", "e" + i);
            int part = Integer.parseInt(c.produce("events", "same-key", "e10").split(":")[0]);
            List<Client.Message> msgs = c.fetchBatch("events", part, 3, 4);
            assertThat(msgs).extracting(Client.Message::value).containsExactly("e3", "e4", "e5", "e6");
            assertThat(msgs).extracting(Client.Message::offset).containsExactly(3L, 4L, 5L, 6L);
            assertThat(c.fetchBatch("events", part, 50, 10)).isEmpty();
        }
    }

    @Test
    void badInputIsAnErrorReplyNotACrash() throws Exception {
        start();
        try (var c = new Client("localhost", node.port(), "p")) {
            assertThatThrownBy(() -> c.produce("../etc", "k", "v")).isInstanceOf(IOException.class).hasMessageContaining("invalid topic name");
            assertThatThrownBy(() -> c.produce("__consumer_offsets", "k", "v")).hasMessageContaining("reserved");
            assertThatThrownBy(() -> c.fetch("orders", 9, 0)).isInstanceOf(IOException.class);       // only 4 partitions
            assertThatThrownBy(() -> c.join("bad/group", "orders", "c1")).isInstanceOf(IOException.class);
            assertThat(c.produce("orders", "k", "still works")).contains(":");                       // connection survived all of that
        }
        assertThat(new String(node.broker.process("NOPE".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8))
                .isEqualTo("ERR unknown command");
        assertThat(new String(node.broker.process("CL_ENDS 1 x y".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8))
                .contains("cluster mode is not enabled");
    }

    @Test
    void helloRejectsStrangeIds() throws Exception {
        start();
        assertThatThrownBy(() -> new Client("localhost", node.port(), "bad/../id")).isInstanceOf(IOException.class);
    }

    @Test
    void manyClientsProducingAtOnceGetUniqueOffsets() throws Exception {
        start();
        var pool = Executors.newFixedThreadPool(8);
        Set<String> seen = java.util.concurrent.ConcurrentHashMap.newKeySet();
        List<Future<?>> futs = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            final int id = t;
            futs.add(pool.submit(() -> {
                try (var c = new Client("localhost", node.port(), "load-" + id)) {
                    for (int i = 0; i < 30; i++) assertThat(seen.add(c.produce("load", "key" + (i % 7), "m" + id + "-" + i))).isTrue();
                }
                return null;
            }));
        }
        for (Future<?> f : futs) f.get();
        pool.shutdown();
        assertThat(seen).hasSize(240);
        long total = 0;
        for (long e : node.broker.endOffsets("load")) total += e;
        assertThat(total).isEqualTo(240);
        assertThat(auditLines().stream().filter(l -> l.contains(" PRODUCE ")).count()).isEqualTo(240);
    }

    // ------------------------------------------------------------------ consumer groups over TCP

    @Test
    void twoConsumersShareATopicAndEveryRecordIsReadExactlyOnce() throws Exception {
        start();
        try (var p = new Client("localhost", node.port(), "producer");
             var c1 = new Client("localhost", node.port(), "billing-c1");
             var c2 = new Client("localhost", node.port(), "billing-c2")) {
            for (int i = 0; i < 40; i++) p.produce("orders", "key" + i, "order-" + i);

            c1.join("billing", "orders", "c1");
            c2.join("billing", "orders", "c2");
            var a1 = c1.join("billing", "orders", "c1");
            var a2 = c2.join("billing", "orders", "c2");
            assertThat(a1.partitions()).containsExactly(0, 1);
            assertThat(a2.partitions()).containsExactly(2, 3);

            Set<String> got = new HashSet<>();
            for (int round = 0; round < 10; round++) {
                for (var m : c1.poll("billing", "orders", "c1", 5).messages()) {
                    assertThat(m.partition()).isIn(0, 1);
                    assertThat(got.add(m.value())).as("duplicate " + m.value()).isTrue();
                }
                for (var m : c2.poll("billing", "orders", "c2", 5).messages()) {
                    assertThat(m.partition()).isIn(2, 3);
                    assertThat(got.add(m.value())).as("duplicate " + m.value()).isTrue();
                }
            }
            assertThat(got).hasSize(40);
        }
        List<String> lines = auditLines();
        long c1Reads = lines.stream().filter(l -> l.contains(" CONSUME actor=tcp:billing-c1@") && l.contains("consumer=c1") && l.contains("group=billing")).count();
        long c2Reads = lines.stream().filter(l -> l.contains(" CONSUME actor=tcp:billing-c2@") && l.contains("consumer=c2") && l.contains("group=billing")).count();
        assertThat(c1Reads + c2Reads).isEqualTo(40);                                // every consumed record is attributed to someone
        assertThat(c1Reads).isGreaterThan(0);
        assertThat(c2Reads).isGreaterThan(0);
        assertThat(lines).anyMatch(l -> l.contains(" ASSIGN ") && l.contains("c1=[0,1];c2=[2,3]"));
    }

    @Test
    void uncommittedWorkIsRedeliveredAndCommittedWorkIsNot() throws Exception {
        start();
        try (var p = new Client("localhost", node.port(), "producer");
             var c = new Client("localhost", node.port(), "worker")) {
            for (int i = 0; i < 8; i++) p.produce("jobs", "k" + i, "job-" + i);
            c.join("g", "jobs", "w1");
            assertThat(c.poll("g", "jobs", "w1", 100).messages()).hasSize(8);       // "crashed" before commit
            c.leave("g", "jobs", "w1");

            c.join("g", "jobs", "w1");                                              // restart: everything comes again
            var again = c.poll("g", "jobs", "w1", 100).messages();
            assertThat(again).hasSize(8);

            Map<Integer, Long> done = c.commit("g", "jobs", "w1");                  // now it is acknowledged
            assertThat(done.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(8);
            c.leave("g", "jobs", "w1");
            c.join("g", "jobs", "w1");
            assertThat(c.poll("g", "jobs", "w1", 100).messages()).isEmpty();
            p.produce("jobs", "k-new", "job-new");
            assertThat(c.poll("g", "jobs", "w1", 100).messages()).extracting(Client.Message::value).containsExactly("job-new");
        }
    }

    @Test
    void committedOffsetsSurviveARestartOfTheBroker() throws Exception {
        start();
        try (var p = new Client("localhost", node.port(), "producer");
             var c = new Client("localhost", node.port(), "worker")) {
            for (int i = 0; i < 6; i++) p.produce("jobs", "k" + i, "job-" + i);
            c.join("g", "jobs", "w1");
            c.poll("g", "jobs", "w1", 100);
            c.commit("g", "jobs", "w1");
        }
        node.stop();

        node = TestSupport.start(tmp, "node-1", null);                              // same data directory
        try (var p = new Client("localhost", node.port(), "producer");
             var c = new Client("localhost", node.port(), "worker")) {
            p.produce("jobs", "k-after", "job-after");
            c.join("g", "jobs", "w1");
            assertThat(c.poll("g", "jobs", "w1", 100).messages()).extracting(Client.Message::value).containsExactly("job-after");
        }
    }

    @Test
    void aConsumerThatGoesSilentLosesItsPartitions() throws Exception {
        node = TestSupport.start(tmp, "node-1", p -> p.getGroup().setSessionTimeoutMs(300));
        try (var c1 = new Client("localhost", node.port(), "c1");
             var c2 = new Client("localhost", node.port(), "c2")) {
            c1.join("g", "t", "c1");
            c2.join("g", "t", "c2");
            int gen = c2.beat("g", "t", "c2");
            long end = System.currentTimeMillis() + 700;
            while (System.currentTimeMillis() < end) { c2.beat("g", "t", "c2"); Thread.sleep(50); }   // only c2 stays alive
            assertThat(c2.beat("g", "t", "c2")).isGreaterThan(gen);
            assertThat(c2.join("g", "t", "c2").partitions()).containsExactly(0, 1, 2, 3);
            assertThatThrownBy(() -> c1.poll("g", "t", "c1", 1)).hasMessageContaining("not a member");
        }
        assertThat(auditLines()).anyMatch(l -> l.contains(" EXPIRE ") && l.contains("consumer=c1"));
    }

    @Test
    void groupSummaryNamesMembersAndLag() throws Exception {
        start();
        try (var p = new Client("localhost", node.port(), "producer");
             var c = new Client("localhost", node.port(), "c1")) {
            for (int i = 0; i < 12; i++) p.produce("t", "k" + i, "v" + i);
            c.join("g", "t", "c1");
            c.poll("g", "t", "c1", 5);
            c.commit("g", "t", "c1");
            var info = node.broker.describeGroup("g");
            assertThat(info.topics()).hasSize(1);
            long lag = info.topics().get(0).partitions().stream().mapToLong(x -> x.lag()).sum();
            assertThat(lag).isEqualTo(7);                                           // 12 produced, 5 committed
            assertThat(info.topics().get(0).members()).extracting(m -> m.consumer()).containsExactly("c1");
        }
    }

    @Test
    void theBrokerReportsStandaloneStatus() throws Exception {
        start();
        var st = node.broker.clusterStatus();
        assertThat(st.enabled()).isFalse();
        assertThat(st.role()).isEqualTo("STANDALONE");
        assertThat(st.nodeId()).isEqualTo("node-1");
    }

    @Test
    void groupCommitCanBeSwitchedOff() throws Exception {
        node = TestSupport.start(tmp, "node-1", p -> p.getGroupCommit().setEnabled(false));
        try (var c = new Client("localhost", node.port(), "p")) {
            String[] first = c.produce("t", "k", "v").split(":");
            String[] second = c.produce("t", "k", "w").split(":");
            assertThat(second[0]).isEqualTo(first[0]);
            assertThat(first[1]).isEqualTo("0");
            assertThat(second[1]).isEqualTo("1");
        }
    }

    @Test
    void auditCanBeDisabledWithoutBreakingAnything() throws Exception {
        node = TestSupport.start(tmp, "node-1", p -> p.getAudit().setEnabled(false));
        try (var c = new Client("localhost", node.port(), "p")) {
            String[] at = c.produce("t", "k", "v").split(":");
            assertThat(c.fetch("t", Integer.parseInt(at[0]), Long.parseLong(at[1]))).isEqualTo("v");
        }
        assertThat(node.audit.isEnabled()).isFalse();
        assertThat(java.nio.file.Files.exists(node.auditFile())).isFalse();         // no audit file was created
    }
}
