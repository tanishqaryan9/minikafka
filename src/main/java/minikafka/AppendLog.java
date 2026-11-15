package minikafka;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

//Modified Reader/Writer Problem
//Durable, append-only log for ONE partition.
//On-disk record:  [ len : 4 ][ crc32 : 4 ][ payload : len ]

public class AppendLog implements Closeable {
    static final int HEADER = 8; // header = bytes of len + crc32
    public static final int INDEX_INTERVAL = 1000; // ek partition ki size
    static final int MAX_RECORD = 16 * 1024 * 1024; // 16 MB record per partition

    private final FileChannel ch;
    private final ReentrantLock lock = new ReentrantLock(); // for one writer/reader at a time
    private final TreeMap<Long, Long> index = new TreeMap<>(); // offset -> byte position
    private long nextOffset = 0; // the line number for the next record
    private long endPosition = 0; //The byte position where the file’s valid data ends

    public AppendLog(Path file) throws IOException {
        this.ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        recover();
    }

    //The in constructor calls this method and suppose if error occurs between writing data
    // then it checks from the start for faulty data and removes them
    //Scan from 0, validate every record, rebuild the index, truncate any torn tail
    private void recover() throws IOException {
        long pos = 0, size = ch.size(), offset = 0;
        while (pos + HEADER <= size) {
            ByteBuffer h = ByteBuffer.allocate(HEADER);
            readFully(h, pos);
            int len = h.getInt(0);
            int storedCrc = h.getInt(4);
            if (len < 0 || len > MAX_RECORD || pos + HEADER + len > size) break; // torn header/payload
            byte[] payload = new byte[len];
            readFully(ByteBuffer.wrap(payload), pos + HEADER);
            if (crc(payload) != storedCrc) break;                                 // corrupt
            if (offset % INDEX_INTERVAL == 0) index.put(offset, pos);
            pos += HEADER + len;
            offset++;
        }
        if (pos < size)
        {
            ch.truncate(pos);
            ch.force(true);
        }
        endPosition = pos;
        nextOffset = offset;
    }

    //Append one record durably. Returns its offset
    public long append(byte[] payload) throws IOException {
        if (payload.length > MAX_RECORD) throw new IllegalArgumentException("record too large");
        ByteBuffer buf = ByteBuffer.allocate(HEADER + payload.length);
        buf.putInt(payload.length).putInt(crc(payload)).put(payload).flip();
        lock.lock();
        try {
            long pos = endPosition;
            while (buf.hasRemaining())
            {
                pos += ch.write(buf, pos);
            }
            ch.force(true);                       // durable before we ack
            long offset = nextOffset;
            if (offset % INDEX_INTERVAL == 0) index.put(offset, endPosition);
            endPosition = pos;
            nextOffset++;
            return offset;
        }
        finally
        {
            lock.unlock();
        }
    }

    //Read the record at the given offset, verifying its CRC. Returns null if not written yet
    public Record read(long offset) throws IOException {
        lock.lock();
        try {
            if (offset < 0 || offset >= nextOffset) return null;
            var e = index.floorEntry(offset);
            long cur = e.getKey(), pos = e.getValue();
            ByteBuffer h = ByteBuffer.allocate(HEADER);
            while (true) {
                h.clear();
                readFully(h, pos);
                int len = h.getInt(0);
                if (cur == offset) {
                    byte[] payload = new byte[len];
                    readFully(ByteBuffer.wrap(payload), pos + HEADER);
                    if (crc(payload) != h.getInt(4))
                        throw new IOException("CRC mismatch at offset " + offset);
                    return new Record(offset, payload);
                }
                pos += HEADER + len;
                cur++;
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    //Append several records with ONE write and ONE fsync (this is what group commit uses).
    //Returns the offset of the first record; the others follow consecutively.
    public long appendAll(List<byte[]> payloads) throws IOException {
        return appendAll(payloads, -1);
    }

    //Append a record only if the log is exactly at expectedOffset (used by replication, where a
    //follower must append in the leader's order). Returns the offset, or -1 if the log is elsewhere.
    public long appendIfNext(long expectedOffset, byte[] payload) throws IOException {
        return appendAll(List.of(payload), expectedOffset);
    }

    //expectedBase < 0 means "append wherever the log ends"
    private long appendAll(List<byte[]> payloads, long expectedBase) throws IOException {
        if (payloads.isEmpty()) throw new IllegalArgumentException("nothing to append");
        int total = 0;
        for (byte[] p : payloads) {
            if (p.length > MAX_RECORD) throw new IllegalArgumentException("record too large");
            total += HEADER + p.length;
        }
        //Build all the bytes BEFORE taking the lock so the locked section stays short
        ByteBuffer buf = ByteBuffer.allocate(total);
        for (byte[] p : payloads) buf.putInt(p.length).putInt(crc(p)).put(p);
        buf.flip();
        lock.lock();
        try {
            if (expectedBase >= 0 && nextOffset != expectedBase) return -1;
            long pos = endPosition;
            while (buf.hasRemaining()) {
                pos += ch.write(buf, pos);
            }
            ch.force(true);                       // one fsync for the whole batch
            long base = nextOffset, off = base, recordPos = endPosition;
            for (byte[] p : payloads) {           // sparse-index checkpoints inside the batch
                if (off % INDEX_INTERVAL == 0) index.put(off, recordPos);
                recordPos += HEADER + p.length;
                off++;
            }
            endPosition = pos;
            nextOffset = base + payloads.size();
            return base;
        } finally {
            lock.unlock();
        }
    }

    //Read up to maxRecords starting at 'from', but never at or beyond 'upTo' (the high watermark).
    //One index lookup, then a sequential scan, so it is far cheaper than calling read() in a loop.
    public List<Record> readBatch(long from, int maxRecords, long upTo) throws IOException {
        lock.lock();
        try {
            long limit = Math.min(upTo, nextOffset);
            List<Record> out = new ArrayList<>();
            if (from < 0 || from >= limit || maxRecords <= 0) return out;
            var e = index.floorEntry(from);
            long cur = e.getKey(), pos = e.getValue();
            ByteBuffer h = ByteBuffer.allocate(HEADER);
            while (cur < limit && out.size() < maxRecords) {
                h.clear();
                readFully(h, pos);
                int len = h.getInt(0);
                if (cur >= from) {
                    byte[] payload = new byte[len];
                    readFully(ByteBuffer.wrap(payload), pos + HEADER);
                    if (crc(payload) != h.getInt(4))
                        throw new IOException("CRC mismatch at offset " + cur);
                    out.add(new Record(cur, payload));
                }
                pos += HEADER + len;
                cur++;
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    public List<Record> readBatch(long from, int maxRecords) throws IOException {
        return readBatch(from, maxRecords, Long.MAX_VALUE);
    }

    //Same result as read(), but ignores the sparse index and scans from byte 0.
    //Only exists so the benchmark can show what the index buys you.
    public Record readNaive(long offset) throws IOException {
        lock.lock();
        try {
            if (offset < 0 || offset >= nextOffset) return null;
            long cur = 0, pos = 0;
            ByteBuffer h = ByteBuffer.allocate(HEADER);
            while (true) {
                h.clear();
                readFully(h, pos);
                int len = h.getInt(0);
                if (cur == offset) {
                    byte[] payload = new byte[len];
                    readFully(ByteBuffer.wrap(payload), pos + HEADER);
                    if (crc(payload) != h.getInt(4))
                        throw new IOException("CRC mismatch at offset " + offset);
                    return new Record(offset, payload);
                }
                pos += HEADER + len;
                cur++;
            }
        } finally {
            lock.unlock();
        }
    }

    //Cut the log back so that 'offset' becomes the next offset to be written.
    //A follower uses this when its tail disagrees with the leader's log.
    public void truncateTo(long offset) throws IOException {
        if (offset < 0) throw new IllegalArgumentException("negative offset");
        lock.lock();
        try {
            if (offset >= nextOffset) return;
            var e = index.floorEntry(offset);
            long cur = e.getKey(), pos = e.getValue();
            ByteBuffer h = ByteBuffer.allocate(HEADER);
            while (cur < offset) {                 // walk headers to find the byte position of 'offset'
                h.clear();
                readFully(h, pos);
                pos += HEADER + h.getInt(0);
                cur++;
            }
            ch.truncate(pos);
            ch.force(true);
            index.tailMap(offset, true).clear();   // forget bookmarks that pointed into the removed part
            endPosition = pos;
            nextOffset = offset;
        } finally {
            lock.unlock();
        }
    }

    //CRC of a payload (public so replication can compare records between nodes)
    public static int crc32(byte[] data) {
        return crc(data);
    }

    public long nextOffset() {
        lock.lock();
        try { return nextOffset; } finally { lock.unlock(); }
    }

    private void readFully(ByteBuffer b, long pos) throws IOException {
        while (b.hasRemaining()) {
            int n = ch.read(b, pos);
            if (n < 0) throw new IOException("unexpected EOF");
            pos += n;
        }
    }

    private static int crc(byte[] data) {
        CRC32 c = new CRC32();
        c.update(data);
        return (int) c.getValue();
    }

    @Override
    public void close() throws IOException { ch.close(); }
}
