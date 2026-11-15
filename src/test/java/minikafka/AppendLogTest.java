package minikafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class AppendLogTest {
    @TempDir Path tmp;

    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static String s(Record r) { return new String(r.payload(), StandardCharsets.UTF_8); }

    @Test
    void appendAndReadAcrossSparseIndexCheckpoints() throws Exception {
        try (var log = new AppendLog(tmp.resolve("a.log"))) {
            for (int i = 0; i < 2500; i++) assertThat(log.append(b("rec-" + i))).isEqualTo(i);
            assertThat(s(log.read(0))).isEqualTo("rec-0");
            assertThat(s(log.read(1500))).isEqualTo("rec-1500");
            assertThat(s(log.read(2499))).isEqualTo("rec-2499");
            assertThat(log.read(2500)).isNull();
        }
    }

    @Test
    void reopenResumesOffsetsAndKeepsData() throws Exception {
        Path f = tmp.resolve("a.log");
        try (var log = new AppendLog(f)) { for (int i = 0; i < 2500; i++) log.append(b("rec-" + i)); }
        try (var log = new AppendLog(f)) {
            assertThat(log.nextOffset()).isEqualTo(2500);
            assertThat(log.append(b("after"))).isEqualTo(2500);
            assertThat(s(log.read(2500))).isEqualTo("after");
        }
    }

    @Test
    void tornTailIsTruncatedOnRecovery() throws Exception {
        Path torn = tmp.resolve("torn.log");
        try (var log = new AppendLog(torn)) { for (int i = 0; i < 10; i++) log.append(b("r" + i)); }
        long size = Files.size(torn);
        try (var ch = FileChannel.open(torn, StandardOpenOption.WRITE)) { ch.truncate(size - 2); }
        try (var log = new AppendLog(torn)) {
            assertThat(log.nextOffset()).isEqualTo(9);
            assertThat(s(log.read(8))).isEqualTo("r8");
            assertThat(log.append(b("new"))).isEqualTo(9);
        }
    }

    @Test
    void crcMismatchStopsRecovery() throws Exception {
        Path bad = tmp.resolve("bad.log");
        try (var log = new AppendLog(bad)) { for (int i = 0; i < 5; i++) log.append(b("data" + i)); }
        byte[] raw = Files.readAllBytes(bad);
        raw[raw.length - 1] ^= 0x55;
        Files.write(bad, raw);
        try (var log = new AppendLog(bad)) { assertThat(log.nextOffset()).isEqualTo(4); }
    }

    @Test
    void concurrentAppendsGetUniqueGapFreeOffsets() throws Exception {
        try (var log = new AppendLog(tmp.resolve("c.log"))) {
            var pool = Executors.newFixedThreadPool(8);
            var seen = ConcurrentHashMap.<Long>newKeySet();
            var futs = new ArrayList<Future<?>>();
            for (int t = 0; t < 8; t++) futs.add(pool.submit(() -> {
                for (int i = 0; i < 50; i++) seen.add(log.append(b("x")));
                return null;
            }));
            for (var f : futs) f.get();
            pool.shutdown();
            assertThat(seen).hasSize(400);
            assertThat(log.nextOffset()).isEqualTo(400);
        }
    }
}
