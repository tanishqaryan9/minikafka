package minikafka.bench;

import minikafka.AppendLog;
import minikafka.GroupCommitter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

//Benchmarks for the two big design decisions of the log. No Spring, no network: just the storage engine.
//
//  1. DURABILITY COST   fsync per record  vs  group commit (many producers share one fsync)
//  2. INDEX BENEFIT     sparse index      vs  scanning the file from the start
//
//Run after "mvn compile":
//  java -cp target/classes minikafka.bench.Bench                       (defaults)
//  java -cp target/classes minikafka.bench.Bench appends=5000 threads=32 payload=200 records=200000 dir=/tmp/mk-bench
//
//Numbers depend on the disk: an fsync costs microseconds on some virtual disks and several milliseconds on others,
//so compare the ROWS with each other, not with numbers from another machine.
public final class Bench {
    private Bench() {}

    //An AppendLog that counts write+fsync rounds, to show how many records each fsync carried
    private static final class CountingLog extends AppendLog {
        final AtomicInteger rounds = new AtomicInteger();

        CountingLog(Path f) throws IOException { super(f); }

        @Override
        public long appendAll(List<byte[]> payloads) throws IOException {
            rounds.incrementAndGet();
            return super.appendAll(payloads);
        }
    }

    private record Result(String name, int ops, double seconds, long[] nanos, String note) {
        double opsPerSec() { return ops / seconds; }

        double pct(double p) {
            long[] s = nanos.clone();
            Arrays.sort(s);
            return s[Math.min(s.length - 1, (int) Math.ceil(p / 100.0 * s.length) - 1)] / 1_000_000.0;
        }

        double mean() { return Arrays.stream(nanos).average().orElse(0) / 1_000_000.0; }
    }

    public static void main(String[] args) throws Exception {
        int appends = 2000, threads = 16, payloadBytes = 100, records = 100_000, reads = 300;
        Path dir = Files.createTempDirectory("minikafka-bench");
        for (String a : args) {
            String[] kv = a.split("=", 2);
            if (kv.length != 2) continue;
            switch (kv[0]) {
                case "appends" -> appends = Integer.parseInt(kv[1]);
                case "threads" -> threads = Integer.parseInt(kv[1]);
                case "payload" -> payloadBytes = Integer.parseInt(kv[1]);
                case "records" -> records = Integer.parseInt(kv[1]);
                case "reads" -> reads = Integer.parseInt(kv[1]);
                case "dir" -> { dir = Path.of(kv[1]); Files.createDirectories(dir); }
                default -> System.err.println("ignoring unknown option " + a);
            }
        }
        byte[] payload = new byte[payloadBytes];
        Arrays.fill(payload, (byte) 'x');

        System.out.printf("MiniKafka benchmark   appends=%d threads=%d payload=%dB records=%d reads=%d%n", appends, threads, payloadBytes, records, reads);
        System.out.println("data directory: " + dir.toAbsolutePath());
        System.out.println("java " + System.getProperty("java.version") + ", " + Runtime.getRuntime().availableProcessors() + " cpus\n");

        // ------------------------------------------------------------------ 1. durability
        List<Result> writes = new ArrayList<>();
        writes.add(oneFsyncPerRecord("1 thread,  fsync per record", dir.resolve("w1.log"), appends, 1, payload));
        writes.add(oneFsyncPerRecord(threads + " threads, fsync per record", dir.resolve("w2.log"), appends, threads, payload));
        writes.add(groupCommit(threads + " threads, group commit", dir.resolve("w3.log"), appends, threads, payload));
        writes.add(groupCommit("1 thread,  group commit", dir.resolve("w4.log"), appends, 1, payload));

        System.out.println("1) WRITES: every record is on disk (fsync) before it is acknowledged");
        System.out.printf("%-34s %10s %9s %9s %9s %9s   %s%n", "mode", "ops/sec", "mean ms", "p50 ms", "p95 ms", "p99 ms", "");
        for (Result r : writes)
            System.out.printf("%-34s %10.0f %9.3f %9.3f %9.3f %9.3f   %s%n", r.name(), r.opsPerSec(), r.mean(), r.pct(50), r.pct(95), r.pct(99), r.note());
        double speedup = writes.get(2).opsPerSec() / writes.get(1).opsPerSec();
        System.out.printf("-> with %d producers, group commit is %.1fx faster than an fsync per record%n%n", threads, speedup);

        // ------------------------------------------------------------------ 2. index
        Path rf = dir.resolve("reads.log");
        try (AppendLog log = new AppendLog(rf)) {
            for (int base = 0; base < records; base += 1000) {
                List<byte[]> batch = new ArrayList<>();
                for (int i = base; i < Math.min(records, base + 1000); i++) batch.add(("record-" + i).getBytes(StandardCharsets.UTF_8));
                log.appendAll(batch);
            }
            System.out.println("2) READS: one record by offset, in a log of " + records + " records (index bookmark every " + AppendLog.INDEX_INTERVAL + ")");
            System.out.printf("%-16s %-22s %12s %12s %9s%n", "offset", "method", "mean us", "p99 us", "speedup");
            //offsets that are NOT exactly on a bookmark, so the index has to scan part of the way (a typical case)
            long[] offsets = {records / 100L + 500, records / 2L + 500, records - 1L};
            for (long off : offsets) {
                Result naive = timeReads(log, off, reads, true);
                Result idx = timeReads(log, off, reads, false);
                System.out.printf("%-16d %-22s %12.1f %12.1f%n", off, "scan from byte 0", naive.mean() * 1000, naive.pct(99) * 1000);
                System.out.printf("%-16s %-22s %12.1f %12.1f %8.1fx%n", "", "sparse index", idx.mean() * 1000, idx.pct(99) * 1000, naive.mean() / Math.max(idx.mean(), 1e-9));
            }
            long t0 = System.nanoTime();
            int got = 0;
            for (long from = 0; from < records; ) {
                var batch = log.readBatch(from, 500);
                if (batch.isEmpty()) break;
                got += batch.size();
                from = batch.get(batch.size() - 1).offset() + 1;
            }
            double sec = (System.nanoTime() - t0) / 1e9;
            System.out.printf("%nsequential read with readBatch(500): %d records in %.3f s = %.0f records/sec%n", got, sec, got / sec);
        }
    }

    private static Result oneFsyncPerRecord(String name, Path file, int total, int threads, byte[] payload) throws Exception {
        try (AppendLog log = new AppendLog(file)) {
            return run(name, total, threads, "", () -> { log.append(payload); });
        }
    }

    private static Result groupCommit(String name, Path file, int total, int threads, byte[] payload) throws Exception {
        try (CountingLog log = new CountingLog(file); GroupCommitter gc = new GroupCommitter(log, 128, 1024, "bench")) {
            Result r = run(name, total, threads, "", () -> { gc.append(payload); });
            String note = log.rounds.get() + " fsyncs, avg batch " + String.format("%.1f", (double) total / Math.max(1, log.rounds.get()));
            return new Result(r.name(), r.ops(), r.seconds(), r.nanos(), note);
        }
    }

    private interface Op { void run() throws Exception; }

    //Runs 'total' operations split over 'threads' threads, timing each one
    private static Result run(String name, int total, int threads, String note, Op op) throws Exception {
        int per = total / threads;
        int ops = per * threads;
        long[][] lat = new long[threads][per];
        CountDownLatch ready = new CountDownLatch(threads), go = new CountDownLatch(1);
        List<Thread> ts = new ArrayList<>();
        Throwable[] failure = {null};
        for (int t = 0; t < threads; t++) {
            final int id = t;
            Thread th = new Thread(() -> {
                try {
                    ready.countDown();
                    go.await();
                    for (int i = 0; i < per; i++) {
                        long s = System.nanoTime();
                        op.run();
                        lat[id][i] = System.nanoTime() - s;
                    }
                } catch (Throwable e) {
                    failure[0] = e;
                }
            });
            ts.add(th);
            th.start();
        }
        ready.await();
        long start = System.nanoTime();
        go.countDown();
        for (Thread th : ts) th.join();
        double seconds = (System.nanoTime() - start) / 1e9;
        if (failure[0] != null) throw new RuntimeException(failure[0]);
        long[] all = new long[ops];
        for (int t = 0; t < threads; t++) System.arraycopy(lat[t], 0, all, t * per, per);
        return new Result(name, ops, seconds, all, note);
    }

    private static Result timeReads(AppendLog log, long offset, int reps, boolean naive) throws IOException {
        long[] lat = new long[reps];
        for (int i = 0; i < 5; i++) { if (naive) log.readNaive(offset); else log.read(offset); }   // warm up
        long start = System.nanoTime();
        for (int i = 0; i < reps; i++) {
            long s = System.nanoTime();
            var r = naive ? log.readNaive(offset) : log.read(offset);
            lat[i] = System.nanoTime() - s;
            if (r == null || r.offset() != offset) throw new IllegalStateException("wrong record");
        }
        return new Result(naive ? "naive" : "index", reps, (System.nanoTime() - start) / 1e9, lat, "");
    }
}
