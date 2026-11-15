package minikafka.cluster;

import minikafka.Client;
import minikafka.Record;
import minikafka.TestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static minikafka.TestSupport.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

//Three real brokers on real sockets: election, replication, failover, repair of a diverged old leader.
class ClusterFailoverTest {
    static final int N = 3;
    static final long WAIT = 15_000;

    @TempDir Path tmp;
    final int[] ports = new int[N];
    final TestSupport.Node[] nodes = new TestSupport.Node[N];

    @BeforeEach
    void ports() throws IOException {
        for (int i = 0; i < N; i++) ports[i] = TestSupport.freePort();
    }

    @AfterEach
    void stopAll() throws IOException {
        for (int i = 0; i < N; i++) if (nodes[i] != null) { nodes[i].stop(); nodes[i] = null; }
    }

    //(Re)start node i with a fixed port and its own data directory, peers = the other nodes
    void startNode(int i) throws IOException {
        nodes[i] = TestSupport.start(tmp.resolve("node" + (i + 1)), "node-" + (i + 1), p -> {
            p.setPort(ports[i]);
            p.setPartitions(2);
            var c = p.getCluster();
            c.setEnabled(true);
            c.setHeartbeatMs(50);
            c.setElectionTimeoutMs(300);
            c.setAckTimeoutMs(1500);
            List<String> peers = new ArrayList<>();
            for (int j = 0; j < N; j++) if (j != i) peers.add("node-" + (j + 1) + "=localhost:" + ports[j]);
            c.setPeers(peers);
        });
    }

    void startAll() throws IOException {
        for (int i = 0; i < N; i++) startNode(i);
    }

    void kill(int i) throws IOException {
        nodes[i].stop();
        nodes[i] = null;
    }

    //Wait until exactly one running node is LEADER and every other running node agrees who it is
    int awaitLeader() throws InterruptedException {
        int[] found = {-1};
        await("a single agreed leader", WAIT, () -> {
            int leader = -1, leaders = 0;
            for (int i = 0; i < N; i++) {
                if (nodes[i] == null) continue;
                if (nodes[i].broker.clusterStatus().role().equals("LEADER")) { leader = i; leaders++; }
            }
            if (leaders != 1) return false;
            for (int i = 0; i < N; i++) {
                if (nodes[i] == null || i == leader) continue;
                var st = nodes[i].broker.clusterStatus();
                if (!st.role().equals("FOLLOWER") || !("node-" + (leader + 1)).equals(st.leaderId())) return false;
            }
            found[0] = leader;
            return true;
        });
        return found[0];
    }

    Client client(int node, String id) throws IOException {
        return new Client("localhost", ports[node], id);
    }

    static List<String> payloads(TestSupport.Node n, String topic, int partition) throws IOException {
        List<String> out = new ArrayList<>();
        for (Record r : n.broker.localRecords(topic, partition, 0, 10_000))
            out.add(new String(r.payload(), StandardCharsets.UTF_8));
        return out;
    }

    static long total(TestSupport.Node n, String topic) {
        long t = 0;
        for (long e : n.broker.localEndOffsets(topic)) t += e;
        return t;
    }

    // ------------------------------------------------------------------ election and replication

    @Test
    void oneLeaderIsElectedAndEveryAcknowledgedWriteReachesEveryNode() throws Exception {
        startAll();
        int leader = awaitLeader();
        Set<String> acked = new HashSet<>();
        try (var c = client(leader, "order-service")) {
            for (int i = 0; i < 30; i++) acked.add(c.produce("orders", "key" + i, "order-" + i));
        }
        assertThat(acked).hasSize(30);
        for (int i = 0; i < N; i++) {
            final int n = i;
            await("node " + n + " has all 30 records", WAIT, () -> total(nodes[n], "orders") == 30);
        }
        for (int part = 0; part < 2; part++) {
            List<String> expected = payloads(nodes[leader], "orders", part);
            for (int i = 0; i < N; i++) assertThat(payloads(nodes[i], "orders", part)).isEqualTo(expected);   // byte-for-byte same logs
        }
    }

    @Test
    void followersRefuseClientsAndSayWhereTheLeaderIs() throws Exception {
        startAll();
        int leader = awaitLeader();
        int follower = (leader + 1) % N;
        try (var c = client(follower, "confused-client")) {
            assertThatThrownBy(() -> c.produce("orders", "k", "v")).isInstanceOf(IOException.class)
                    .hasMessageContaining("NOT_LEADER").hasMessageContaining("localhost:" + ports[leader]);
            assertThatThrownBy(() -> c.fetch("orders", 0, 0)).hasMessageContaining("NOT_LEADER");
            assertThatThrownBy(() -> c.join("g", "orders", "c1")).hasMessageContaining("NOT_LEADER");
        }
    }

    @Test
    void consumersOnlySeeCommittedRecordsAndReadsAreServedByTheLeader() throws Exception {
        startAll();
        int leader = awaitLeader();
        try (var c = client(leader, "p")) {
            String[] at = c.produce("orders", "k", "v1").split(":");
            assertThat(c.fetch("orders", Integer.parseInt(at[0]), Long.parseLong(at[1]))).isEqualTo("v1");
            assertThat(c.fetch("orders", Integer.parseInt(at[0]), Long.parseLong(at[1]) + 1)).isNull();
        }
    }

    @Test
    void aSingleNodeClusterLeadsImmediately() throws Exception {
        nodes[0] = TestSupport.start(tmp.resolve("solo"), "node-1", p -> {
            p.setPartitions(2);
            p.getCluster().setEnabled(true);
            p.getCluster().setElectionTimeoutMs(300);
            p.getCluster().setHeartbeatMs(50);
        });
        await("solo node becomes leader", WAIT, () -> nodes[0].broker.clusterStatus().role().equals("LEADER"));
        try (var c = new Client("localhost", nodes[0].port(), "p")) {
            assertThat(c.produce("t", "k", "v")).contains(":");
        }
    }

    // ------------------------------------------------------------------ failover

    @Test
    void killingTheLeaderLosesNothingThatWasAcknowledgedAndTheOldLeaderCatchesUp() throws Exception {
        startAll();
        int oldLeader = awaitLeader();
        List<String> where = new ArrayList<>();
        try (var c = client(oldLeader, "writer")) {
            for (int i = 0; i < 12; i++) where.add(c.produce("orders", "key" + i, "before-" + i));
        }
        long termBefore = nodes[oldLeader].broker.clusterStatus().term();
        kill(oldLeader);

        int newLeader = awaitLeader();                                        // the two survivors elect one between them
        assertThat(newLeader).isNotEqualTo(oldLeader);
        assertThat(nodes[newLeader].broker.clusterStatus().term()).isGreaterThan(termBefore);

        try (var c = client(newLeader, "reader-after-failover")) {
            for (int i = 0; i < 12; i++) {                                    // every acknowledged record is still there
                String[] at = where.get(i).split(":");
                assertThat(c.fetch("orders", Integer.parseInt(at[0]), Long.parseLong(at[1]))).isEqualTo("before-" + i);
            }
            for (int i = 0; i < 5; i++) c.produce("orders", "key" + i, "after-" + i);   // and writes keep working
        }

        startNode(oldLeader);                                                 // the old leader comes back as a follower
        int again = awaitLeader();
        assertThat(again).isEqualTo(newLeader);
        await("old leader caught up", WAIT, () -> total(nodes[oldLeader], "orders") == 17);
        for (int part = 0; part < 2; part++)
            assertThat(payloads(nodes[oldLeader], "orders", part)).isEqualTo(payloads(nodes[newLeader], "orders", part));
    }

    @Test
    void aFollowerThatWasDownMissesWritesAndIsBroughtUpToDate() throws Exception {
        startAll();
        int leader = awaitLeader();
        int sleeper = (leader + 1) % N;
        kill(sleeper);
        try (var c = client(leader, "writer")) {
            for (int i = 0; i < 20; i++) c.produce("orders", "key" + i, "while-away-" + i);   // two of three nodes are enough
        }
        startNode(sleeper);
        awaitLeader();
        await("sleeper caught up", WAIT, () -> total(nodes[sleeper], "orders") == 20);
        for (int part = 0; part < 2; part++)
            assertThat(payloads(nodes[sleeper], "orders", part)).isEqualTo(payloads(nodes[leader], "orders", part));
    }

    @Test
    void aLeaderThatCannotReachAMajorityRefusesWritesAndStepsDown() throws Exception {
        startAll();
        int leader = awaitLeader();
        for (int i = 0; i < N; i++) if (i != leader) kill(i);
        await("isolated leader steps down", WAIT, () -> !nodes[leader].broker.clusterStatus().role().equals("LEADER"));
        try (var c = client(leader, "p")) {
            assertThatThrownBy(() -> c.produce("orders", "k", "v")).hasMessageContaining("NOT_LEADER");
        }
    }

    @Test
    void anUnreplicatedRecordIsReportedAsFailedAndLaterOverwrittenByTheNewLeader() throws Exception {
        startAll();
        final int oldLeader = awaitLeader();
        final int partition = Math.floorMod("k".hashCode(), 2);
        try (var c = client(oldLeader, "writer")) {
            for (int i = 0; i < 3; i++) c.produce("orders", "k", "common-" + i);
        }
        for (int i = 0; i < N; i++) {
            final int n = i;
            await("all nodes have the common prefix", WAIT, () -> total(nodes[n], "orders") == 3);
        }

        int a = (oldLeader + 1) % N, b = (oldLeader + 2) % N;
        kill(a);
        kill(b);                                                              // the leader is alone now
        long before = nodes[oldLeader].broker.localEndOffsets("orders")[partition];
        try (var c = client(oldLeader, "writer")) {
            assertThatThrownBy(() -> c.produce("orders", "k", "EXTRA-never-acknowledged")).isInstanceOf(IOException.class);
        } catch (IOException e) {
            // could not even connect: leader already stepped down; handled by the assumption below
        }
        Assumptions.assumeTrue(nodes[oldLeader].broker.localEndOffsets("orders")[partition] == before + 1,
                "leader stepped down before the write; nothing diverged in this run");
        assertThat(payloads(nodes[oldLeader], "orders", partition)).contains("EXTRA-never-acknowledged");   // it IS in the old leader's log
        kill(oldLeader);

        startNode(a);
        startNode(b);
        final int newLeader = awaitLeader();                                  // the survivors never saw EXTRA
        try (var c = client(newLeader, "writer")) {
            c.produce("orders", "k", "new-1");
            c.produce("orders", "k", "new-2");
        }

        startNode(oldLeader);                                                 // back with a log that disagrees
        awaitLeader();
        final List<String> expected = payloads(nodes[newLeader], "orders", partition);
        assertThat(expected).containsExactly("common-0", "common-1", "common-2", "new-1", "new-2");
        await("old leader's log repaired", WAIT, () -> {
            try { return payloads(nodes[oldLeader], "orders", partition).equals(expected); }
            catch (IOException e) { return false; }
        });
        assertThat(nodes[oldLeader].auditLines()).anyMatch(l -> l.contains(" TRUNCATE ") && l.contains("log diverged from leader"));
    }

    @Test
    void consumerProgressSurvivesFailover() throws Exception {
        startAll();
        int leader = awaitLeader();
        Set<String> all = new HashSet<>();
        try (var p = client(leader, "producer")) {
            for (int i = 0; i < 8; i++) { p.produce("orders", "k" + i, "order-" + i); all.add("order-" + i); }
        }
        Set<String> firstBatch = new HashSet<>();
        try (var c = client(leader, "billing-c1")) {
            c.join("billing", "orders", "c1");
            for (var m : c.poll("billing", "orders", "c1", 3).messages()) firstBatch.add(m.value());
            assertThat(firstBatch).hasSize(3);
            c.commit("billing", "orders", "c1");
        }
        kill(leader);
        int newLeader = awaitLeader();

        Set<String> rest = new HashSet<>();
        try (var c = client(newLeader, "billing-c1")) {
            assertThatThrownBy(() -> c.poll("billing", "orders", "c1", 10)).hasMessageContaining("join");   // group state is per leader
            c.join("billing", "orders", "c1");
            for (var m : c.poll("billing", "orders", "c1", 100).messages()) rest.add(m.value());
        }
        assertThat(rest).hasSize(5);                                          // committed records are NOT delivered again
        assertThat(rest).doesNotContainAnyElementsOf(firstBatch);
        Set<String> union = new HashSet<>(firstBatch);
        union.addAll(rest);
        assertThat(union).isEqualTo(all);
    }

    @Test
    void theAuditTrailExplainsTheElectionAndTheReplication() throws Exception {
        startAll();
        int leader = awaitLeader();
        try (var c = client(leader, "order-service")) { c.produce("orders", "alice", "deposit"); }
        int follower = (leader + 1) % N;
        await("follower logged the replicated record", WAIT, () -> {
            try {
                return nodes[follower].auditLines().stream().anyMatch(l -> l.contains(" REPLICATED actor=node-" + (leader + 1)) && l.contains("preview") == false
                        && l.contains("topic=orders"));
            } catch (IOException e) { return false; }
        });
        assertThat(nodes[leader].auditLines()).anyMatch(l -> l.contains(" LEADER_ELECTED actor=node-" + (leader + 1)));
        assertThat(nodes[leader].auditLines()).anyMatch(l -> l.contains(" PRODUCE actor=tcp:order-service@") && l.contains("[node-" + (leader + 1) + "]"));
        assertThat(nodes[follower].auditLines()).anyMatch(l -> l.contains(" VOTE_GRANTED ") || l.contains(" ELECTION_START "));
    }
}
