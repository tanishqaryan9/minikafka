package minikafka.group;

import minikafka.Record;
import minikafka.cluster.Replicator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetStoreTest {
    @TempDir Path tmp;

    @Test
    void unknownIsMinusOneAndLatestCommitWins() throws Exception {
        try (var s = new OffsetStore(tmp.resolve("o.log"), Replicator.STANDALONE)) {
            assertThat(s.get("g", "orders", 0)).isEqualTo(-1);
            s.commit("g", "orders", Map.of(0, 5L, 1, 7L));
            s.commit("g", "orders", Map.of(0, 9L));
            assertThat(s.get("g", "orders", 0)).isEqualTo(9);
            assertThat(s.get("g", "orders", 1)).isEqualTo(7);
            assertThat(s.get("other", "orders", 0)).isEqualTo(-1);            // groups and topics do not mix
            assertThat(s.get("g", "payments", 0)).isEqualTo(-1);
        }
    }

    @Test
    void commitsSurviveReopeningTheStore() throws Exception {
        Path f = tmp.resolve("o.log");
        try (var s = new OffsetStore(f, Replicator.STANDALONE)) {
            s.commit("g", "orders", Map.of(0, 5L, 1, 7L));
            s.commit("g", "orders", Map.of(0, 6L));
        }
        try (var s = new OffsetStore(f, Replicator.STANDALONE)) {             // replay of the log rebuilds the map
            assertThat(s.get("g", "orders", 0)).isEqualTo(6);
            assertThat(s.get("g", "orders", 1)).isEqualTo(7);
            assertThat(s.endOffset()).isEqualTo(3);                           // three commit records were written
        }
    }

    @Test
    void manyCommitsReplayCorrectlyAcrossReadBatches() throws Exception {
        Path f = tmp.resolve("o.log");
        try (var s = new OffsetStore(f, Replicator.STANDALONE)) {
            for (int i = 1; i <= 1300; i++) s.commit("g", "t", Map.of(i % 3, (long) i));
        }
        try (var s = new OffsetStore(f, Replicator.STANDALONE)) {
            assertThat(s.get("g", "t", 0)).isEqualTo(1299);
            assertThat(s.get("g", "t", 1)).isEqualTo(1300);
            assertThat(s.get("g", "t", 2)).isEqualTo(1298);
        }
    }

    @Test
    void replicaPathAppendsInOrderAndTruncationRebuildsTheMap() throws Exception {
        try (var leader = new OffsetStore(tmp.resolve("l.log"), Replicator.STANDALONE);
             var follower = new OffsetStore(tmp.resolve("f.log"), Replicator.STANDALONE)) {
            leader.commit("g", "t", Map.of(0, 1L));
            leader.commit("g", "t", Map.of(0, 2L));
            leader.commit("g", "t", Map.of(0, 3L));
            List<Record> recs = leader.read(0, 10);
            assertThat(recs).hasSize(3);

            assertThat(follower.appendReplica(1, recs.get(1).payload())).isEqualTo(-1);   // gap: refused
            for (Record r : recs) assertThat(follower.appendReplica(r.offset(), r.payload())).isEqualTo(r.offset());
            assertThat(follower.get("g", "t", 0)).isEqualTo(3);

            follower.truncateTo(1);                                           // forget the last two commits
            assertThat(follower.get("g", "t", 0)).isEqualTo(1);
            assertThat(follower.endOffset()).isEqualTo(1);
        }
    }

    @Test
    void anUnreplicatedCommitIsReportedToTheCaller() throws Exception {
        Replicator failing = new Replicator() {
            @Override public boolean isClustered() { return true; }
            @Override public void requireLeader() { }
            @Override public void replicate(String t, int p, long o, byte[] b) throws java.io.IOException {
                throw new java.io.IOException("no majority");
            }
            @Override public long highWatermark(String t, int p, long e) { return e; }
        };
        try (var s = new OffsetStore(tmp.resolve("o.log"), failing)) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> s.commit("g", "t", Map.of(0, 1L)))
                    .isInstanceOf(java.io.IOException.class).hasMessageContaining("no majority");
        }
    }

    @Test
    void ignoresRecordsItCannotParse() throws Exception {
        Path f = tmp.resolve("o.log");
        try (var log = new minikafka.AppendLog(f)) {
            log.append("garbage".getBytes(StandardCharsets.UTF_8));
            log.append("g\tt\t0\t4".getBytes(StandardCharsets.UTF_8));
        }
        try (var s = new OffsetStore(f, Replicator.STANDALONE)) {
            assertThat(s.get("g", "t", 0)).isEqualTo(4);
        }
    }
}
