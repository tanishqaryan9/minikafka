package minikafka;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

//Minimal blocking client for the Broker's framed protocol
public class Client implements Closeable {
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    public Client(String host, int port) throws IOException {
        socket = new Socket(host, port);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    private String call(byte[] req) throws IOException {
        Frames.send(out, req);
        return new String(Frames.read(in), StandardCharsets.UTF_8);
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
        String r = call(("FETCH " + topic + " " + partition + " " + offset).getBytes(StandardCharsets.UTF_8));
        if (r.equals("EMPTY")) return null;
        if (!r.startsWith("OK ")) throw new IOException(r);
        return r.substring(3);
    }

    @Override
    public void close() throws IOException { socket.close(); }
}
