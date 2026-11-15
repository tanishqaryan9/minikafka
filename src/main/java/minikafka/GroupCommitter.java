package minikafka;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

//Group commit for ONE partition (producer-consumer problem, for real this time).
//
//  producer threads --put()--> [ BoundedBuffer ] --take()+drainTo()--> ONE writer thread
//                                                                        |
//                                         appendAll(batch) = 1 write + 1 fsync
//                                                                        |
//                                     every producer in the batch is woken with its offset
//
//A producer is only told "done" AFTER the fsync, so durability is exactly the same as AppendLog.append().
//The win: 50 concurrent producers share one fsync instead of paying for 50.
//The bounded buffer gives back-pressure: if the disk is slow the buffer fills and put() blocks the producer.
public final class GroupCommitter implements Closeable {
    private record Pending(byte[] payload, CompletableFuture<Long> result) {}

    private static final long WAIT_SECONDS = 30;

    private final AppendLog log;
    private final int maxBatch;
    private final BoundedBuffer<Pending> queue;
    private final Thread writer;
    private volatile boolean closed;

    public GroupCommitter(AppendLog log, int maxBatch, int queueCapacity, String name) {
        this.log = log;
        this.maxBatch = Math.max(1, maxBatch);
        this.queue = new BoundedBuffer<>(Math.max(1, queueCapacity));
        this.writer = new Thread(this::writeLoop, "group-commit-" + name);
        this.writer.setDaemon(true);
        this.writer.start();
    }

    //Blocks until the record is durable on disk. Returns its offset.
    public long append(byte[] payload) throws IOException {
        if (payload.length > AppendLog.MAX_RECORD) throw new IllegalArgumentException("record too large");
        if (closed) throw new IOException("group committer closed");
        Pending p = new Pending(payload, new CompletableFuture<>());
        try {
            queue.put(p);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while queueing");
        }
        // close() may have run between the check above and put(); make sure nobody waits forever
        if (closed) p.result().completeExceptionally(new IOException("group committer closed"));
        try {
            return p.result().get(WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while waiting for fsync");
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof IOException io) throw io;
            if (c instanceof RuntimeException re) throw re;
            throw new IOException(c);
        } catch (TimeoutException e) {
            throw new IOException("group commit timed out", e);
        }
    }

    private void writeLoop() {
        List<Pending> batch = new ArrayList<>(maxBatch);
        try {
            while (true) {
                batch.clear();
                batch.add(queue.take());                    // sleep until there is work
                queue.drainTo(batch, maxBatch - 1);         // grab whatever piled up meanwhile
                flush(batch);
            }
        } catch (InterruptedException e) {
            // close() asked us to stop
        } finally {
            failQueued();
        }
    }

    private void flush(List<Pending> batch) {
        try {
            List<byte[]> payloads = new ArrayList<>(batch.size());
            for (Pending p : batch) payloads.add(p.payload());
            long base = log.appendAll(payloads);
            for (int i = 0; i < batch.size(); i++) batch.get(i).result().complete(base + i);
        } catch (Throwable t) {
            for (Pending p : batch) p.result().completeExceptionally(t);
        }
    }

    private void failQueued() {
        List<Pending> rest = new ArrayList<>();
        queue.drainTo(rest, Integer.MAX_VALUE);
        for (Pending p : rest) p.result().completeExceptionally(new IOException("group committer closed"));
    }

    @Override
    public void close() {
        closed = true;
        writer.interrupt();
        try {
            writer.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
