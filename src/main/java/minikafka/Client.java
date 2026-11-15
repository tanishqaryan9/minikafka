package minikafka;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

//Minimal blocking client for the Broker's framed protocol
public class Client implements Closeable {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    //A record handed back by fetchBatch() or poll()
    public record Message(int partition, long offset, String value) {}
    public record Assignment(int generation, List<Integer> partitions) {}
    public record Polled(int generation, List<Message> messages) {}

    public Client(String host, int port) throws IOException {
        socket = new Socket(host, port);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    //Same, but tells the broker who you are: the id appears on every audit-log line made by this connection
    public Client(String host, int port, String clientId) throws IOException {
        this(host, port);
        String r = call("HELLO " + clientId);
        if (!r.equals("OK")) {
            socket.close();
            throw new IOException(r);
        }
    }

    private byte[] callRaw(byte[] req) throws IOException {
        Frames.send(out, req);
        return Frames.read(in);
    }

    private String call(byte[] req) throws IOException {
        return new String(callRaw(req), StandardCharsets.UTF_8);
    }

    private String call(String req) throws IOException {
        return call(req.getBytes(StandardCharsets.UTF_8));
    }

    //Returns "partition:offset"
    public String produce(String topic, String key, String value) throws IOException {
        String r = call(("PRODUCE " + topic + " " + key + "\n" + value).getBytes(StandardCharsets.UTF_8));
        if (!r.startsWith("OK ")) throw new IOException(r);
        String[] p = r.split(" ");
        return p[1] + ":" + p[2];
    }

    //Returns the payload, or null if nothing at that offset yet
    public String fetch(String topic, int partition, long offset) throws IOException {
        String r = call("FETCH " + topic + " " + partition + " " + offset);
        if (r.equals("EMPTY")) return null;
        if (!r.startsWith("OK ")) throw new IOException(r);
        return r.substring(3);
    }

    //Up to max records of one partition starting at 'from' (you keep track of where you are)
    public List<Message> fetchBatch(String topic, int partition, long from, int max) throws IOException {
        return parseRecords(callRaw(("FETCHB " + topic + " " + partition + " " + from + " " + max)
                .getBytes(StandardCharsets.UTF_8))).messages();
    }

    // ------------------------------------------------------------------ consumer groups

    public Assignment join(String group, String topic, String consumer) throws IOException {
        String[] p = ok(call("JOIN " + group + " " + topic + " " + consumer)).split(" ");
        List<Integer> parts = new ArrayList<>();
        if (!p[1].equals("-")) for (String s : p[1].split(",")) parts.add(Integer.parseInt(s));
        return new Assignment(Integer.parseInt(p[0]), parts);
    }

    public void leave(String group, String topic, String consumer) throws IOException {
        ok(call("LEAVE " + group + " " + topic + " " + consumer));
    }

    //Keeps the session alive; returns the group generation (it changes when a rebalance happened)
    public int beat(String group, String topic, String consumer) throws IOException {
        return Integer.parseInt(ok(call("BEAT " + group + " " + topic + " " + consumer)));
    }

    //Next records of the partitions assigned to this consumer
    public Polled poll(String group, String topic, String consumer, int max) throws IOException {
        return parseRecords(callRaw(("POLL " + group + " " + topic + " " + consumer + " " + max)
                .getBytes(StandardCharsets.UTF_8)));
    }

    //Commit the current position of every partition this consumer owns. Returns partition -> committed offset.
    public Map<Integer, Long> commit(String group, String topic, String consumer) throws IOException {
        return parseOffsets(ok(call("COMMIT " + group + " " + topic + " " + consumer)));
    }

    //Commit explicit offsets (partition -> next offset to read)
    public Map<Integer, Long> commit(String group, String topic, String consumer, Map<Integer, Long> offsets) throws IOException {
        StringJoiner sj = new StringJoiner(",");
        new TreeMap<>(offsets).forEach((k, v) -> sj.add(k + ":" + v));
        return parseOffsets(ok(call("COMMIT " + group + " " + topic + " " + consumer + " " + sj)));
    }

    // ------------------------------------------------------------------ parsing

    //Strips the leading "OK " (or "OK"); anything else is an error reply
    private static String ok(String reply) throws IOException {
        if (reply.equals("OK")) return "";
        if (!reply.startsWith("OK ")) throw new IOException(reply);
        return reply.substring(3);
    }

    private static Map<Integer, Long> parseOffsets(String s) {
        Map<Integer, Long> m = new TreeMap<>();
        if (s.isEmpty() || s.equals("-")) return m;
        for (String pair : s.split(",")) {
            String[] kv = pair.split(":");
            m.put(Integer.parseInt(kv[0]), Long.parseLong(kv[1]));
        }
        return m;
    }

    //"OK <generation> <count>\n" + count x [int partition][long offset][int length][payload]
    private static Polled parseRecords(byte[] reply) throws IOException {
        int nl = -1;
        for (int i = 0; i < reply.length; i++) if (reply[i] == '\n') { nl = i; break; }
        String head = new String(reply, 0, nl < 0 ? reply.length : nl, StandardCharsets.UTF_8);
        if (!head.startsWith("OK ") || nl < 0) throw new IOException(head);
        String[] h = head.split(" ");
        int generation = Integer.parseInt(h[1]), count = Integer.parseInt(h[2]);
        DataInputStream body = new DataInputStream(new ByteArrayInputStream(reply, nl + 1, reply.length - nl - 1));
        List<Message> msgs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int partition = body.readInt();
            long offset = body.readLong();
            byte[] payload = new byte[body.readInt()];
            body.readFully(payload);
            msgs.add(new Message(partition, offset, new String(payload, StandardCharsets.UTF_8)));
        }
        return new Polled(generation, msgs);
    }

    @Override
    public void close() throws IOException { socket.close(); }
}
