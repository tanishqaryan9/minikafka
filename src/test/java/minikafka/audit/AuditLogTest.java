package minikafka.audit;

import minikafka.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AuditLogTest {
    @TempDir Path tmp;

    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    private AuditLog log(Path f, long maxBytes, int backups, int preview, boolean perRecord) {
        return new AuditLog(f, "node-1", maxBytes, backups, preview, perRecord, 100);
    }

    @Test
    void producedLineSaysWhoAddedWhatWhere() throws Exception {
        Path f = tmp.resolve("logs").resolve("audit.log");                    // parent directory is created for us
        try (var a = log(f, 0, 0, 40, true)) {
            a.produced("tcp:order-service@127.0.0.1:5000", "orders", 2, 7, "alice", b("deposit"));
        }
        List<String> lines = Files.readAllLines(f);
        assertThat(lines).hasSize(1);
        String l = lines.get(0);
        assertThat(l).matches("\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}Z \\[node-1\\] PRODUCE .*");
        assertThat(l).contains("actor=tcp:order-service@127.0.0.1:5000", "topic=orders", "partition=2", "offset=7",
                "key=alice", "bytes=7", "preview=deposit");
    }

    @Test
    void consumedLineSaysWhoReadWhat() throws Exception {
        Path f = tmp.resolve("audit.log");
        try (var a = log(f, 0, 0, 40, true)) {
            a.consumed("rest:billing-1@10.0.0.5", "billing", "billing-1", "orders", 0,
                    List.of(new Record(7, b("deposit")), new Record(8, b("withdraw cash"))));
        }
        List<String> lines = Files.readAllLines(f);
        assertThat(lines).hasSize(2);                                         // one line per record
        assertThat(lines.get(0)).contains(" CONSUME ", "actor=rest:billing-1@10.0.0.5", "group=billing",
                "consumer=billing-1", "topic=orders", "partition=0", "offset=7", "preview=deposit");
        assertThat(lines.get(1)).contains("offset=8", "preview=\"withdraw cash\"");   // values with spaces are quoted
    }

    @Test
    void batchModeWritesOneSummaryLine() throws Exception {
        Path f = tmp.resolve("audit.log");
        try (var a = log(f, 0, 0, 40, false)) {
            a.consumed("tcp:c@h", null, null, "orders", 1, List.of(new Record(3, b("a")), new Record(4, b("b")), new Record(5, b("c"))));
        }
        List<String> lines = Files.readAllLines(f);
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).contains("from=3", "to=5", "count=3", "bytes=3").doesNotContain("group=");
    }

    @Test
    void previewIsOffWhenZeroAndTruncatedWhenLong() throws Exception {
        Path f1 = tmp.resolve("a.log"), f2 = tmp.resolve("b.log");
        try (var a = log(f1, 0, 0, 0, true); var c = log(f2, 0, 0, 5, true)) {
            a.produced("x", "t", 0, 0, "k", b("secret-data"));
            c.produced("x", "t", 0, 0, "k", b("abcdefghij"));
        }
        assertThat(Files.readString(f1)).doesNotContain("preview").doesNotContain("secret");
        assertThat(Files.readString(f2)).contains("preview=abcde...");
    }

    @Test
    void controlCharactersAndQuotesCannotBreakALineOrFakeAnotherEvent() throws Exception {
        Path f = tmp.resolve("audit.log");
        try (var a = log(f, 0, 0, 80, true)) {
            a.produced("rest:ev\"il@x", "t", 0, 0, "k", b("line1\nline2 PRODUCE actor=forged"));
        }
        assertThat(Files.readAllLines(f)).hasSize(1);                         // the newline in the payload did not start a new line
    }

    @Test
    void recentCanBeFilteredByTypeActorTopicAndGroup() throws Exception {
        try (var a = log(tmp.resolve("audit.log"), 0, 0, 10, true)) {
            a.produced("tcp:p1@h", "orders", 0, 0, "k", b("a"));
            a.produced("tcp:p2@h", "payments", 0, 0, "k", b("b"));
            a.consumed("tcp:c1@h", "billing", "c1", "orders", 0, List.of(new Record(0, b("a"))));
            a.record("COMMIT", "tcp:c1@h", "group", "billing", "topic", "orders", "offsets", "{0=1}");

            assertThat(a.recent(100, null, null, null, null)).hasSize(4);
            assertThat(a.recent(100, "produce", null, null, null)).hasSize(2);
            assertThat(a.recent(100, null, "p2", null, null)).hasSize(1);
            assertThat(a.recent(100, null, null, "orders", null)).hasSize(3);
            assertThat(a.recent(100, null, null, null, "billing")).hasSize(2);
            assertThat(a.recent(1, null, null, null, null)).hasSize(1);
            assertThat(a.recent(1, null, null, null, null).get(0).type()).isEqualTo("COMMIT");   // newest events are kept
        }
    }

    @Test
    void rotatesIntoBackupsAndKeepsOnlyTheConfiguredNumber() throws Exception {
        Path f = tmp.resolve("audit.log");
        try (var a = log(f, 300, 2, 0, true)) {
            for (int i = 0; i < 60; i++) a.produced("tcp:p@h", "orders", 0, i, "k", b("x"));
        }
        assertThat(Files.exists(f)).isTrue();
        assertThat(Files.exists(tmp.resolve("audit.log.1"))).isTrue();
        assertThat(Files.exists(tmp.resolve("audit.log.2"))).isTrue();
        assertThat(Files.exists(tmp.resolve("audit.log.3"))).isFalse();       // oldest were dropped
    }

    @Test
    void appendsToAnExistingFileAfterARestart() throws Exception {
        Path f = tmp.resolve("audit.log");
        try (var a = log(f, 0, 0, 0, true)) { a.record("NODE_START", "n"); }
        try (var a = log(f, 0, 0, 0, true)) { a.record("NODE_START", "n"); }
        assertThat(Files.readAllLines(f)).hasSize(2);
    }

    @Test
    void disabledLogRecordsNothingAndClosedLogIgnoresLateEvents() throws Exception {
        var off = AuditLog.disabled();
        off.produced("x", "t", 0, 0, "k", b("a"));
        assertThat(off.isEnabled()).isFalse();
        assertThat(off.recent(10, null, null, null, null)).isEmpty();

        var a = log(tmp.resolve("audit.log"), 0, 0, 0, true);
        a.close();
        a.record("LATE", "x");                                                // must not throw
        assertThat(Files.readString(tmp.resolve("audit.log"))).isEmpty();
    }
}
