package minikafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GroupCommitterTest {
    @TempDir Path tmp;

    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @Test
    void concurrentProducersGetUniqueGapFreeOffsetsAndEveryRecordIsReadable() throws Exception {
        Path f = tmp.resolve("p.log");
        try (var log = new AppendLog(f); var gc = new GroupCommitter(log, 64, 256, "t")) {
            var pool = Executors.newFixedThreadPool(16);
            Set<Long> seen = ConcurrentHashMap.newKeySet();
            List<Future<?>> futs = new ArrayList<>();
            for (int t = 0; t < 16; t++) {
                final int id = t;
                futs.add(pool.submit(() -> {
                    for (int i = 0; i < 40; i++) {
                        long off = gc.append(b("t" + id + "-" + i));
                        // what we were told must be what is stored at that offset
                        assertThat(new String(log.read(off).payload(), StandardCharsets.UTF_8)).isEqualTo("t" + id + "-" + i);
                        seen.add(off);
                    }
                    return null;
                }));
            }
            for (Future<?> fu : futs) fu.get();
            pool.shutdown();
            assertThat(seen).hasSize(640);
            assertThat(log.nextOffset()).isEqualTo(640);
            for (long o = 0; o < 640; o++) assertThat(seen).contains(o);
        }
        try (var log = new AppendLog(f)) {                                    // durable: still there after reopening
            assertThat(log.nextOffset()).isEqualTo(640);
        }
    }

    @Test
    void sequentialProducerStillWorksAndOffsetsAreInOrder() throws Exception {
        try (var log = new AppendLog(tmp.resolve("p.log")); var gc = new GroupCommitter(log, 8, 16, "t")) {
            for (int i = 0; i < 50; i++) assertThat(gc.append(b("x" + i))).isEqualTo(i);
        }
    }

    @Test
    void oversizedPayloadFailsOnlyThatProducer() throws Exception {
        try (var log = new AppendLog(tmp.resolve("p.log")); var gc = new GroupCommitter(log, 8, 16, "t")) {
            assertThatThrownBy(() -> gc.append(new byte[AppendLog.MAX_RECORD + 1])).isInstanceOf(IllegalArgumentException.class);
            assertThat(gc.append(b("ok"))).isEqualTo(0);                      // the writer thread is unharmed
        }
    }

    @Test
    void appendAfterCloseFailsInsteadOfHanging() throws Exception {
        try (var log = new AppendLog(tmp.resolve("p.log"))) {
            var gc = new GroupCommitter(log, 8, 16, "t");
            assertThat(gc.append(b("a"))).isEqualTo(0);
            gc.close();
            assertThatThrownBy(() -> gc.append(b("b"))).isInstanceOf(IOException.class);
        }
    }

    //An AppendLog that counts how many write+fsync rounds it performed
    private static final class CountingLog extends AppendLog {
        final AtomicInteger rounds = new AtomicInteger();

        CountingLog(Path f) throws IOException { super(f); }

        @Override
        public long appendAll(List<byte[]> payloads) throws IOException {
            rounds.incrementAndGet();
            return super.appendAll(payloads);
        }
    }

    @Test
    void manyProducersShareFewFsyncs() throws Exception {
        try (var log = new CountingLog(tmp.resolve("p.log")); var gc = new GroupCommitter(log, 128, 512, "t")) {
            var pool = Executors.newFixedThreadPool(32);
            List<Future<?>> futs = new ArrayList<>();
            for (int t = 0; t < 32; t++) futs.add(pool.submit(() -> {
                for (int i = 0; i < 25; i++) gc.append(b("r"));
                return null;
            }));
            for (Future<?> fu : futs) fu.get();
            pool.shutdown();
            assertThat(log.nextOffset()).isEqualTo(800);
            // 800 records, 32 producers waiting at a time: they must have been written in far fewer rounds than records
            assertThat(log.rounds.get()).isLessThan(600);
        }
    }
}
