package minikafka;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

//Length-prefix framing: [len : 4][payload]. TCP is a byte stream, so we mark boundaries
public final class Frames {
    static final int MAX_FRAME = 16 * 1024 * 1024;
    private Frames() {}

    public static void send(DataOutputStream out, byte[] payload) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    public static byte[] read(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > MAX_FRAME) throw new IOException("bad frame length " + len);
        byte[] payload = new byte[len];
        in.readFully(payload);
        return payload;
    }
}
