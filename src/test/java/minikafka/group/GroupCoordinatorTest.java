package minikafka.group;

import minikafka.Record;
import minikafka.audit.AuditLog;
import minikafka.cluster.Replicator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GroupCoordinatorTest {
    @TempDir Path tmp;

    //In-memory topics: topic -> partition -> payloads (the list index is the offset)
    static final class FakeLogs implements GroupCoordinator.Partitions {
        final int partitions;
        final Map<String, List<List<String>>> data = new HashMap<>();

        FakeLogs(int partitions) { this.partitions = partitions; }

        List<String> part(String topic, int p) {
            return data.computeIfAbsent(topic, t -> {
                List<List<String>> l = new ArrayList<>();
                for (int i = 0; i < partitions; i++) l.add(new ArrayList<>());
                return l;
            }).get(p);
        }

        void add(String topic, int partition, String... values) {
            for (String v : values) part(topic, partition).add(v);
        }

        @Override public int partitionCount(String topic) { part(topic, 0); return partitions; }

        @Override public List<Record> read(String topic, int partition, long from, int max) {
            List<String> l = part(topic, partition);
            List<Record> out = new ArrayList<>();
            for (long o = from; o < l.size() && out.size() < max; o++)
                out.add(new Record(o, l.get((int) o).getBytes(StandardCharsets.UTF_8)));
            return out;
        }

        @Override public long endOffset(String topic, int partition) { return part(topic, partition).size(); }
    }

    FakeLogs logs;
    OffsetStore offsets;
    AuditLog audit;
    AtomicLong clock;
    GroupCoordinator gc;
    Path auditFile;

    @BeforeEach
    void setUp() throws Exception {
        logs = new FakeLogs(4);
        offsets = new OffsetStore(tmp.resolve("offsets.log"), Replicator.STANDALONE);
        auditFile = tmp.resolve("audit.log");
        audit = new AuditLog(auditFile, "n1", 0, 0, 20, true, 500);
        clock = new AtomicLong(1_000);
        gc = new GroupCoordinator(logs, offsets, audit, 1_000, clock::get);
    }

    @AfterEach
    void tearDown() throws Exception {
        audit.close();
        offsets.close();
    }

    static List<String> values(GroupCoordinator.PollResult r) {
        List<String> l = new ArrayList<>();
        for (var rec : r.records()) l.add(new String(rec.payload(), StandardCharsets.UTF_8));
        return l;
    }

    void fill(int perPartition) {
        for (int p = 0; p < 4; p++)
            for (int i = 0; i < perPartition; i++) logs.add("orders", p, "p" + p + "-" + i);
    }

    // ------------------------------------------------------------------ assignment

    @Test
    void aSingleConsumerOwnsEveryPartition() throws Exception {
        var r = gc.join("g", "orders", "c1", "actor");
        assertThat(r.partitions()).containsExactly(0, 1, 2, 3);
        assertThat(r.generation()).isEqualTo(1);
    }

    @Test
    void rangeAssignmentSplitsPartitionsAndBumpsTheGeneration() throws Exception {
        gc.join("g", "orders", "c1", "a");
        gc.join("g", "orders", "c2", "a");
        var c1 = gc.join("g", "orders", "c1", "a");                           // rejoin = "what do I own now?"
        var c2 = gc.join("g", "orders", "c2", "a");
        assertThat(c1.partitions()).containsExactly(0, 1);
        assertThat(c2.partitions()).containsExactly(2, 3);
        assertThat(c1.generation()).isEqualTo(2);
        assertThat(c2.generation()).isEqualTo(2);
    }

    @Test
    void joiningTwiceDoesNotCauseARebalance() throws Exception {
        gc.join("g", "orders", "c1", "a");
        int gen = gc.join("g", "orders", "c1", "a").generation();
        assertThat(gen).isEqualTo(1);
    }

    @Test
    void threeConsumersOnFourPartitionsGetTwoOneOne() throws Exception {
        gc.join("g", "orders", "c1", "a");
        gc.join("g", "orders", "c2", "a");
        gc.join("g", "orders", "c3", "a");
        assertThat(gc.join("g", "orders", "c1", "a").partitions()).hasSize(2);
        assertThat(gc.join("g", "orders", "c2", "a").partitions()).hasSize(1);
        assertThat(gc.join("g", "orders", "c3", "a").partitions()).hasSize(1);
    }

    @Test
    void surplusConsumersSitIdle() throws Exception {
        for (int i = 1; i <= 5; i++) gc.join("g", "orders", "c" + i, "a");
        int idle = 0;
        Set<Integer> all = new HashSet<>();
        for (int i = 1; i <= 5; i++) {
            var p = gc.join("g", "orders", "c" + i, "a").partitions();
            if (p.isEmpty()) idle++;
            all.addAll(p);
        }
        assertThat(idle).isEqualTo(1);
        assertThat(all).containsExactlyInAnyOrder(0, 1, 2, 3);                // still every partition owned exactly once
    }

    @Test
    void groupsAndTopicsAreIndependent() throws Exception {
        gc.join("g1", "orders", "c1", "a");
        gc.join("g2", "orders", "c1", "a");
        assertThat(gc.join("g1", "orders", "c1", "a").partitions()).hasSize(4);
        assertThat(gc.join("g2", "orders", "c1", "a").partitions()).hasSize(4);
        assertThat(gc.groups()).containsExactly("g1", "g2");
    }

    @Test
    void leavingHandsPartitionsToTheRemainingMembers() throws Exception {
        gc.join("g", "orders", "c1", "a");
        gc.join("g", "orders", "c2", "a");
        gc.leave("g", "orders", "c1", "a");
        assertThat(gc.join("g", "orders", "c2", "a").partitions()).containsExactly(0, 1, 2, 3);
    }

    // ------------------------------------------------------------------ polling

    @Test
    void pollOnlyReturnsOwnPartitionsAndAdvances() throws Exception {
        fill(5);
        gc.join("g", "orders", "c1", "a");
        gc.join("g", "orders", "c2", "a");
        var first = gc.poll("g", "orders", "c1", 100, "a");
        assertThat(first.records()).hasSize(10);                              // partitions 0 and 1, five records each
        assertThat(first.records()).allMatch(r -> r.partition() == 0 || r.partition() == 1);
        assertThat(gc.poll("g", "orders", "c1", 100, "a").records()).isEmpty();   // nothing new
        logs.add("orders", 1, "late");
        assertThat(values(gc.poll("g", "orders", "c1", 100, "a"))).containsExactly("late");
    }

    @Test
    void pollRespectsMaxAndEachRecordIsDeliveredOnce() throws Exception {
        fill(5);
        gc.join("g", "orders", "c1", "a");
        Set<String> seen = new HashSet<>();
        int polls = 0;
        while (true) {
            var r = gc.poll("g", "orders", "c1", 3, "a");
            if (r.records().isEmpty()) break;
            assertThat(r.records().size()).isLessThanOrEqualTo(3);
            for (String v : values(r)) assertThat(seen.add(v)).as("duplicate " + v).isTrue();
            polls++;
        }
        assertThat(seen).hasSize(20);
        assertThat(polls).isGreaterThanOrEqualTo(7);
    }

    @Test
    void smallPollsRotateAcrossPartitionsSoNoPartitionStarves() throws Exception {
        fill(50);
        gc.join("g", "orders", "c1", "a");
        Set<Integer> touched = new HashSet<>();
        for (int i = 0; i < 4; i++) for (var r : gc.poll("g", "orders", "c1", 1, "a").records()) touched.add(r.partition());
        assertThat(touched).containsExactlyInAnyOrder(0, 1, 2, 3);
    }

    // ------------------------------------------------------------------ at-least-once

    @Test
    void uncommittedRecordsComeBackAfterARebalance() throws Exception {
        fill(3);
        gc.join("g", "orders", "c1", "a");
        assertThat(gc.poll("g", "orders", "c1", 100, "a").records()).hasSize(12);
        assertThat(gc.poll("g", "orders", "c1", 100, "a").records()).isEmpty();
        gc.join("g", "orders", "c2", "a");                                    // rebalance: positions reset to committed (none)
        assertThat(gc.poll("g", "orders", "c1", 100, "a").records()).hasSize(6);   // c1 now owns 0,1 and starts over
    }

    @Test
    void committedRecordsAreNotDeliveredAgain() throws Exception {
        fill(3);
        gc.join("g", "orders", "c1", "a");
        gc.poll("g", "orders", "c1", 100, "a");
        var committed = gc.commit("g", "orders", "c1", null, "a");
        assertThat(committed).containsEntry(0, 3L).containsEntry(3, 3L);
        gc.join("g", "orders", "c2", "a");                                    // rebalance
        assertThat(gc.poll("g", "orders", "c1", 100, "a").records()).isEmpty();
        logs.add("orders", 0, "fresh");
        assertThat(values(gc.poll("g", "orders", "c1", 100, "a"))).containsExactly("fresh");
    }

    @Test
    void committedOffsetsSurviveRestartingTheWholeBroker() throws Exception {
        fill(4);
        gc.join("g", "orders", "c1", "a");
        gc.poll("g", "orders", "c1", 100, "a");
        gc.commit("g", "orders", "c1", null, "a");
        offsets.close();

        offsets = new OffsetStore(tmp.resolve("offsets.log"), Replicator.STANDALONE);   // "restart"
        gc = new GroupCoordinator(logs, offsets, audit, 1_000, clock::get);
        logs.add("orders", 2, "after-restart");
        gc.join("g", "orders", "c1", "a");
        assertThat(values(gc.poll("g", "orders", "c1", 100, "a"))).containsExactly("after-restart");
    }

    @Test
    void aDifferentGroupStartsFromTheBeginning() throws Exception {
        fill(2);
        gc.join("g1", "orders", "c1", "a");
        gc.poll("g1", "orders", "c1", 100, "a");
        gc.commit("g1", "orders", "c1", null, "a");
        gc.join("g2", "orders", "c1", "a");
        assertThat(gc.poll("g2", "orders", "c1", 100, "a").records()).hasSize(8);
    }

    // ------------------------------------------------------------------ commit rules

    @Test
    void explicitCommitIsCheckedForOwnershipAndRange() throws Exception {
        fill(3);
        gc.join("g", "orders", "c1", "a");
        gc.join("g", "orders", "c2", "a");                                    // c1 owns 0,1  c2 owns 2,3
        assertThat(gc.commit("g", "orders", "c1", Map.of(0, 2L), "a")).containsEntry(0, 2L);
        assertThatThrownBy(() -> gc.commit("g", "orders", "c1", Map.of(2, 1L), "a"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("not assigned");
        assertThatThrownBy(() -> gc.commit("g", "orders", "c1", Map.of(0, 99L), "a"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("out of range");
        assertThatThrownBy(() -> gc.commit("g", "orders", "c1", Map.of(0, -1L), "a"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(offsets.get("g", "orders", 0)).isEqualTo(2);               // only the valid commit went through
    }

    @Test
    void anOutsiderCannotPollOrCommit() throws Exception {
        assertThatThrownBy(() -> gc.poll("nope", "orders", "c1", 10, "a")).isInstanceOf(IllegalStateException.class);
        gc.join("g", "orders", "c1", "a");
        assertThatThrownBy(() -> gc.poll("g", "orders", "stranger", 10, "a"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("not a member");
        assertThatThrownBy(() -> gc.commit("g", "orders", "stranger", null, "a")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> gc.leave("g", "orders", "stranger", "a")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> gc.heartbeat("g", "orders", "stranger")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void namesAreValidated() {
        assertThatThrownBy(() -> gc.join("bad name", "orders", "c1", "a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> gc.join("g", "orders", "c 1", "a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> gc.join("g", "orders", "../x", "a")).isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------ sessions

    @Test
    void aSilentConsumerExpiresAndItsPartitionsMove() throws Exception {
        gc.join("g", "orders", "c1", "a");
        gc.join("g", "orders", "c2", "a");
        int before = gc.heartbeat("g", "orders", "c2");
        clock.addAndGet(600);
        gc.heartbeat("g", "orders", "c2");                                    // c2 stays alive
        clock.addAndGet(600);                                                 // c1 has been silent for 1200 ms > 1000 ms
        int after = gc.heartbeat("g", "orders", "c2");
        assertThat(after).isGreaterThan(before);
        assertThat(gc.join("g", "orders", "c2", "a").partitions()).containsExactly(0, 1, 2, 3);
        assertThatThrownBy(() -> gc.poll("g", "orders", "c1", 10, "a")).hasMessageContaining("not a member");
        assertThat(Files.readString(auditFile)).contains(" EXPIRE ", "consumer=c1");
    }

    @Test
    void pollingKeepsASessionAlive() throws Exception {
        gc.join("g", "orders", "c1", "a");
        for (int i = 0; i < 5; i++) {
            clock.addAndGet(800);
            gc.poll("g", "orders", "c1", 1, "a");                             // activity, not only heartbeats, counts
        }
        assertThat(gc.heartbeat("g", "orders", "c1")).isEqualTo(1);
    }

    @Test
    void resetForgetsMembershipButNotCommittedOffsets() throws Exception {
        fill(2);
        gc.join("g", "orders", "c1", "a");
        gc.poll("g", "orders", "c1", 100, "a");
        gc.commit("g", "orders", "c1", null, "a");
        gc.reset();
        assertThatThrownBy(() -> gc.poll("g", "orders", "c1", 10, "a")).isInstanceOf(IllegalStateException.class);
        gc.join("g", "orders", "c1", "a");
        assertThat(gc.poll("g", "orders", "c1", 10, "a").records()).isEmpty();   // resumes after the committed offsets
    }

    // ------------------------------------------------------------------ describe and audit

    @Test
    void describeShowsOwnersCommittedPositionAndLag() throws Exception {
        fill(5);
        gc.join("g", "orders", "c1", "a");
        gc.join("g", "orders", "c2", "a");
        gc.poll("g", "orders", "c1", 3, "a");
        gc.commit("g", "orders", "c1", Map.of(0, 2L), "a");

        var info = gc.describe("g");
        assertThat(info.topics()).hasSize(1);
        var t = info.topics().get(0);
        assertThat(t.members()).extracting(GroupCoordinator.MemberInfo::consumer).containsExactly("c1", "c2");
        var p0 = t.partitions().get(0);
        assertThat(p0.owner()).isEqualTo("c1");
        assertThat(p0.committed()).isEqualTo(2);
        assertThat(p0.end()).isEqualTo(5);
        assertThat(p0.lag()).isEqualTo(3);
        var p2 = t.partitions().get(2);
        assertThat(p2.owner()).isEqualTo("c2");
        assertThat(p2.committed()).isEqualTo(-1);                             // never committed
        assertThat(p2.lag()).isEqualTo(5);
    }

    @Test
    void theAuditTrailSaysWhoJoinedWhoGotWhatWhoConsumedAndWhoCommitted() throws Exception {
        logs.add("orders", 0, "deposit");
        gc.join("billing", "orders", "c1", "tcp:billing-c1@10.0.0.7:4000");
        gc.poll("billing", "orders", "c1", 10, "tcp:billing-c1@10.0.0.7:4000");
        gc.commit("billing", "orders", "c1", null, "tcp:billing-c1@10.0.0.7:4000");
        String all = Files.readString(auditFile);
        assertThat(all).contains(" JOIN actor=tcp:billing-c1@10.0.0.7:4000 group=billing topic=orders consumer=c1");
        assertThat(all).contains(" ASSIGN ", "assignment=\"c1=[0,1,2,3]\"", "generation=1");
        assertThat(all).contains(" CONSUME actor=tcp:billing-c1@10.0.0.7:4000 group=billing consumer=c1 topic=orders partition=0 offset=0",
                "preview=deposit");
        assertThat(all).contains(" COMMIT actor=tcp:billing-c1@10.0.0.7:4000", "offsets=\"{0=1,1=0,2=0,3=0}\"");
    }

    @Test
    void anUnreadablePartitionSurfacesAsAnIoErrorAndDoesNotCorruptPositions() throws Exception {
        fill(2);
        var broken = new GroupCoordinator(new GroupCoordinator.Partitions() {
            boolean failOnce = true;
            @Override public int partitionCount(String t) { return 4; }
            @Override public List<Record> read(String t, int p, long from, int max) throws IOException {
                if (failOnce) { failOnce = false; throw new IOException("disk error"); }
                return logs.read(t, p, from, max);
            }
            @Override public long endOffset(String t, int p) { return logs.endOffset(t, p); }
        }, offsets, audit, 1_000, clock::get);
        broken.join("g", "orders", "c1", "a");
        assertThatThrownBy(() -> broken.poll("g", "orders", "c1", 100, "a")).isInstanceOf(IOException.class);
        assertThat(broken.poll("g", "orders", "c1", 100, "a").records()).hasSize(8);   // nothing was skipped
    }
}
