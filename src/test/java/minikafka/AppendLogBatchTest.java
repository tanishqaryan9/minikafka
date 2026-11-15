package minikafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

//The methods added for group commit, consumers and replication
class AppendLogBatchTest {
    @TempDir Path tmp;

    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static String s(Record r) { return new String(r.payload(), StandardCharsets.UTF_8); }

    static List<byte[]> batch(String prefix, int from, int count) {
        List<byte[]> l = new ArrayList<>();
        for (int i = 0; i < count; i++) l.add(b(prefix + (from + i)));
        return l;
    }

    @Test
    void appendAllReturnsFirstOffsetAndKeepsIndexCheckpointsInsideTheBatch() throws Exception {
        try (var log = new AppendLog(tmp.resolve("a.log"))) {
            assertThat(log.appendAll(batch("r", 0, 700))).isEqualTo(0);
            assertThat(log.appendAll(batch("r", 700, 700))).isEqualTo(700);   // offset 1000 is in the middle of this batch
            assertThat(log.nextOffset()).isEqualTo(1400);
            assertThat(s(log.read(1000))).isEqualTo("r1000");                 // found through the bookmark made inside the batch
            assertThat(s(log.read(1399))).isEqualTo("r1399");
        }
    }

    @Test
    void batchSurvivesReopen() throws Exception {
        Path f = tmp.resolve("a.log");
        try (var log = new AppendLog(f)) { log.appendAll(batch("r", 0, 1500)); }
        try (var log = new AppendLog(f)) {
            assertThat(log.nextOffset()).isEqualTo(1500);
            assertThat(s(log.read(1234))).isEqualTo("r1234");
        }
    }

    @Test
    void readBatchStartsAnywhereAndRespectsMaxAndUpTo() throws Exception {
        try (var log = new AppendLog(tmp.resolve("a.log"))) {
            log.appendAll(batch("r", 0, 2500));
            List<Record> mid = log.readBatch(1998, 5);
            assertThat(mid).hasSize(5);
            assertThat(mid.get(0).offset()).isEqualTo(1998);
            assertThat(s(mid.get(4))).isEqualTo("r2002");
            assertThat(log.readBatch(2495, 100)).hasSize(5);                  // stops at the end of the log
            assertThat(log.readBatch(10, 100, 13)).hasSize(3);                // high watermark 13: offsets 10, 11, 12
            assertThat(log.readBatch(2500, 10)).isEmpty();
            assertThat(log.readBatch(-1, 10)).isEmpty();
            assertThat(log.readBatch(0, 0)).isEmpty();
            assertThat(log.readBatch(20, 5, 20)).isEmpty();                   // nothing below the watermark
        }
    }

    @Test
    void naiveScanAndIndexedReadAgree() throws Exception {
        try (var log = new AppendLog(tmp.resolve("a.log"))) {
            log.appendAll(batch("r", 0, 3500));
            for (long off : new long[]{0, 1, 999, 1000, 1001, 2345, 3499}) {
                assertThat(s(log.readNaive(off))).isEqualTo(s(log.read(off)));
            }
            assertThat(log.readNaive(3500)).isNull();
        }
    }

    @Test
    void appendIfNextOnlyAppendsAtTheExpectedOffset() throws Exception {
        try (var log = new AppendLog(tmp.resolve("a.log"))) {
            assertThat(log.appendIfNext(0, b("a"))).isEqualTo(0);
            assertThat(log.appendIfNext(1, b("b"))).isEqualTo(1);
            assertThat(log.appendIfNext(5, b("gap"))).isEqualTo(-1);          // log is at 2, not 5
            assertThat(log.appendIfNext(1, b("dup"))).isEqualTo(-1);          // already have offset 1
            assertThat(log.nextOffset()).isEqualTo(2);
        }
    }

    @Test
    void truncateToCutsTheTailAndTheLogKeepsWorking() throws Exception {
        Path f = tmp.resolve("a.log");
        try (var log = new AppendLog(f)) {
            log.appendAll(batch("old", 0, 2200));
            log.truncateTo(1500);                                             // drops offsets 1500..2199 and the bookmark at 2000
            assertThat(log.nextOffset()).isEqualTo(1500);
            assertThat(log.read(1500)).isNull();
            assertThat(s(log.read(1499))).isEqualTo("old1499");
            assertThat(log.append(b("new"))).isEqualTo(1500);
            assertThat(s(log.read(1500))).isEqualTo("new");
            log.appendAll(batch("x", 0, 600));                                // crosses offset 2000 again: bookmark is rebuilt
            assertThat(s(log.read(2000))).isEqualTo("x499");
            log.truncateTo(99999);                                            // beyond the end: nothing happens
            assertThat(log.nextOffset()).isEqualTo(2101);
        }
        try (var log = new AppendLog(f)) {                                    // the truncation is on disk, not just in memory
            assertThat(log.nextOffset()).isEqualTo(2101);
            assertThat(s(log.read(1500))).isEqualTo("new");
        }
    }

    @Test
    void truncateToZeroEmptiesTheLog() throws Exception {
        try (var log = new AppendLog(tmp.resolve("a.log"))) {
            log.appendAll(batch("r", 0, 10));
            log.truncateTo(0);
            assertThat(log.nextOffset()).isEqualTo(0);
            assertThat(log.append(b("again"))).isEqualTo(0);
            assertThat(s(log.read(0))).isEqualTo("again");
            assertThatThrownBy(() -> log.truncateTo(-1)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void oversizedRecordRejectedInBatchToo() throws Exception {
        try (var log = new AppendLog(tmp.resolve("a.log"))) {
            List<byte[]> l = new ArrayList<>();
            l.add(b("fine"));
            l.add(new byte[AppendLog.MAX_RECORD + 1]);
            assertThatThrownBy(() -> log.appendAll(l)).isInstanceOf(IllegalArgumentException.class);
            assertThat(log.nextOffset()).isEqualTo(0);                        // nothing was written
        }
    }
}
