package minikafka.audit;

import minikafka.Record;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

//Audit trail in its OWN file (default logs/audit.log), separate from the application log.
//One line per event, always: time, node, EVENT, actor, then key=value pairs.
//
//  2026-11-14T10:15:30.123Z [node-1] PRODUCE actor=tcp:order-service@127.0.0.1:51234 topic=orders partition=0 offset=7 key=alice bytes=7 preview="deposit"
//  2026-11-14T10:15:31.002Z [node-1] CONSUME actor=rest:billing-c1@127.0.0.1 group=billing consumer=c1 topic=orders partition=0 offset=7 bytes=7 preview="deposit"
//
//"actor" answers "by whom": <protocol>:<client id>@<remote address>.
//The last few thousand events are also kept in memory so /api/audit can answer without reading the file.
//Writing is best-effort: an audit failure never fails a produce or a fetch.
public final class AuditLog implements Closeable {
    public record Event(long seq, String time, String node, String type, String actor, Map<String, String> fields) {}

    private static final System.Logger LOG = System.getLogger("minikafka.audit");
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private final boolean enabled;
    private final Path file;
    private final String node;
    private final long maxBytes;
    private final int maxBackups;
    private final int previewChars;
    private final boolean perRecordConsume;
    private final int memoryEvents;
    private final ArrayDeque<Event> recent = new ArrayDeque<>();
    private BufferedWriter out;
    private long size;
    private long seq;

    public AuditLog(Path file, String node, long maxBytes, int maxBackups,
                    int previewChars, boolean perRecordConsume, int memoryEvents) {
        this.enabled = file != null;
        this.file = file;
        this.node = node;
        this.maxBytes = maxBytes;
        this.maxBackups = Math.max(0, maxBackups);
        this.previewChars = Math.max(0, previewChars);
        this.perRecordConsume = perRecordConsume;
        this.memoryEvents = Math.max(1, memoryEvents);
        if (enabled) open();
    }

    //An AuditLog that records nothing (audit.enabled=false)
    public static AuditLog disabled() {
        return new AuditLog(null, "-", 0, 0, 0, false, 1);
    }

    public boolean isEnabled() { return enabled; }
    public Path file() { return file; }

    public synchronized long fileSize() { return size; }

    // ------------------------------------------------------------------ what happened

    //A record was appended to a partition
    public void produced(String actor, String topic, int partition, long offset, String key, byte[] payload) {
        record("PRODUCE", actor,
                "topic", topic, "partition", String.valueOf(partition), "offset", String.valueOf(offset),
                "key", key, "bytes", String.valueOf(payload.length), "preview", preview(payload));
    }

    //Records were handed to a reader. group/consumer are null for a plain FETCH (no consumer group).
    public void consumed(String actor, String group, String consumer, String topic, int partition, List<Record> recs) {
        if (recs.isEmpty()) return;
        if (perRecordConsume) {
            for (Record r : recs) {
                record("CONSUME", actor, "group", group, "consumer", consumer,
                        "topic", topic, "partition", String.valueOf(partition), "offset", String.valueOf(r.offset()),
                        "bytes", String.valueOf(r.payload().length), "preview", preview(r.payload()));
            }
        } else {
            long bytes = 0;
            for (Record r : recs) bytes += r.payload().length;
            record("CONSUME", actor, "group", group, "consumer", consumer,
                    "topic", topic, "partition", String.valueOf(partition),
                    "from", String.valueOf(recs.get(0).offset()),
                    "to", String.valueOf(recs.get(recs.size() - 1).offset()),
                    "count", String.valueOf(recs.size()), "bytes", String.valueOf(bytes));
        }
    }

    //Generic event. kv = key1, value1, key2, value2 ... (null values are skipped)
    public synchronized void record(String type, String actor, String... kv) {
        if (!enabled || out == null) return;       // disabled, or already closed
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            if (kv[i + 1] != null) fields.put(kv[i], kv[i + 1]);
        }
        Instant now = Instant.now();
        Event ev = new Event(++seq, TIME.format(now), node, type, actor == null ? "-" : actor, fields);
        recent.addLast(ev);
        while (recent.size() > memoryEvents) recent.removeFirst();
        try {
            write(format(ev));
        } catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "audit write failed: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------ querying (for /api/audit)

    //Most recent matching events, oldest first. Null filters match everything.
    public synchronized List<Event> recent(int limit, String type, String actor, String topic, String group) {
        List<Event> all = new ArrayList<>();
        for (Event e : recent) {
            if (type != null && !type.equalsIgnoreCase(e.type())) continue;
            if (actor != null && !e.actor().contains(actor)) continue;
            if (topic != null && !topic.equals(e.fields().get("topic"))) continue;
            if (group != null && !group.equals(e.fields().get("group"))) continue;
            all.add(e);
        }
        int from = Math.max(0, all.size() - Math.max(1, limit));
        return new ArrayList<>(all.subList(from, all.size()));
    }

    // ------------------------------------------------------------------ file handling

    private String format(Event e) {
        StringBuilder sb = new StringBuilder(160);
        sb.append(e.time()).append(" [").append(e.node()).append("] ").append(e.type())
          .append(" actor=").append(quote(e.actor()));
        for (Map.Entry<String, String> f : e.fields().entrySet())
            sb.append(' ').append(f.getKey()).append('=').append(quote(f.getValue()));
        return sb.append('\n').toString();
    }

    //Values with spaces, quotes or '=' are wrapped in "..." so every line stays easy to parse
    private static String quote(String v) {
        boolean plain = !v.isEmpty();
        for (int i = 0; i < v.length() && plain; i++) {
            char c = v.charAt(i);
            if (c <= ' ' || c == '"' || c == '=' || c == '\\' || c == 0x7f) plain = false;
        }
        if (plain) return v.length() > 200 ? v.substring(0, 200) : v;
        StringBuilder sb = new StringBuilder("\"");
        int n = Math.min(v.length(), 200);
        for (int i = 0; i < n; i++) {
            char c = v.charAt(i);
            if (c == '"' || c == '\\') sb.append('\\').append(c);
            else if (c < ' ' || c == 0x7f) sb.append('.');
            else sb.append(c);
        }
        return sb.append('"').toString();
    }

    //First few characters of the payload, with unprintable bytes shown as '.'; null when previews are off
    private String preview(byte[] payload) {
        if (previewChars == 0) return null;
        String s = new String(payload, 0, Math.min(payload.length, previewChars * 4), StandardCharsets.UTF_8);
        if (s.length() > previewChars) s = s.substring(0, previewChars) + "...";
        return s;
    }

    private void open() {
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            size = Files.exists(file) ? Files.size(file) : 0;
            out = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("cannot open audit log " + file + ": " + e.getMessage(), e);
        }
    }

    private void write(String line) throws IOException {
        out.write(line);
        out.flush();                                  // so `tail -f logs/audit.log` shows events immediately
        size += line.getBytes(StandardCharsets.UTF_8).length;
        if (maxBytes > 0 && size >= maxBytes) rotate();
    }

    //audit.log -> audit.log.1 -> audit.log.2 ... (oldest dropped)
    private void rotate() throws IOException {
        out.close();
        if (maxBackups == 0) {
            Files.deleteIfExists(file);
        } else {
            Files.deleteIfExists(backup(maxBackups));
            for (int i = maxBackups - 1; i >= 1; i--) {
                if (Files.exists(backup(i))) Files.move(backup(i), backup(i + 1), StandardCopyOption.REPLACE_EXISTING);
            }
            Files.move(file, backup(1), StandardCopyOption.REPLACE_EXISTING);
        }
        size = 0;
        out = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private Path backup(int i) {
        return file.resolveSibling(file.getFileName() + "." + i);
    }

    @Override
    public synchronized void close() throws IOException {
        if (out != null) {
            out.close();
            out = null;
        }
    }
}
