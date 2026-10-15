package minikafka;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

//Modified Reader/Writer Problem
//Durable, append-only log for ONE partition.
//On-disk record:  [ len : 4 ][ crc32 : 4 ][ payload : len ]

public class AppendLog implements Closeable {
    static final int HEADER = 8; // header = bytes of len + crc32
    static final int INDEX_INTERVAL = 1000; // ek partition ki size
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
