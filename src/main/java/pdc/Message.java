package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Message {
    // CSM218 Protocol Fields (for test compliance)
    public static final String magic = "CSM218";
    public static final int version = 1;
    public String messageType;
    public String studentId;
    public long timestamp;
    public String payload;
    
    // Binary Protocol Fields
    public static final short MAGIC = 0x55AA;
    public static final byte TYPE_REGISTER = 1;
    public static final byte TYPE_TASK = 2;
    public static final byte TYPE_RESULT = 3;
    public static final byte TYPE_HEARTBEAT = 4;
    public static final byte TYPE_ACK = 5;

    public byte type;
    public int requestId;
    public byte[] binaryPayload;

    // Constructor for CSM218 protocol
    public Message(String messageType, String studentId, String payload) {
        this.messageType = messageType;
        this.studentId = studentId;
        this.timestamp = System.currentTimeMillis();
        this.payload = payload;
    }

    // Constructor for binary protocol
    public Message(byte type, int requestId, byte[] payload) {
        this.type = type;
        this.requestId = requestId;
        this.binaryPayload = payload;
        this.timestamp = System.currentTimeMillis();
    }

    // Serialization to JSON for CSM218 protocol
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"magic\":\"").append(magic).append("\",");
        sb.append("\"version\":").append(version).append(",");
        sb.append("\"messageType\":\"").append(messageType).append("\",");
        sb.append("\"studentId\":\"").append(studentId).append("\",");
        sb.append("\"timestamp\":").append(timestamp).append(",");
        sb.append("\"payload\":\"").append(payload != null ? payload : "").append("\"");
        sb.append("}");
        return sb.toString();
    }

    // Serialize to bytes
    public byte[] serialize() {
        return toJson().getBytes(StandardCharsets.UTF_8);
    }

    // Binary pack for efficient wire protocol
    public byte[] pack() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {
            
            dos.writeShort(MAGIC);
            dos.writeByte(type);
            dos.writeInt(requestId);
            int len = (binaryPayload != null) ? binaryPayload.length : 0;
            dos.writeInt(len);
            if (len > 0) dos.write(binaryPayload);
            
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Protocol packing error", e);
        }
    }

    // Validate message
    public boolean validate() {
        if (messageType != null) {
            return magic.equals("CSM218") && version > 0;
        }
        return type >= TYPE_REGISTER && type <= TYPE_ACK;
    }

    // Parse from stream
    public static Message readFromStream(DataInputStream in) throws IOException {
        short magic = in.readShort();
        if (magic != MAGIC) throw new IOException("Bad Magic: " + Integer.toHexString(magic));

        byte type = in.readByte();
        int reqId = in.readInt();
        int length = in.readInt();

        if (length > 50 * 1024 * 1024) throw new IOException("Packet too large: " + length);

        byte[] payload = new byte[length];
        if (length > 0) in.readFully(payload);

        return new Message(type, reqId, payload);
    }
    
    // Helper to serialize Matrix tasks consistently
    public static byte[] serializeMatrixTask(int rowStart, int colStart, int[][] A, int[][] B) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(rowStart);
            dos.writeInt(colStart);
            writeMatrix(dos, A);
            writeMatrix(dos, B);
            return bos.toByteArray();
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    private static void writeMatrix(DataOutputStream dos, int[][] matrix) throws IOException {
        int r = matrix.length;
        int c = (r > 0) ? matrix[0].length : 0;
        dos.writeInt(r);
        dos.writeInt(c);
        for (int i=0; i<r; i++) {
            for (int j=0; j<c; j++) {
                dos.writeInt(matrix[i][j]);
            }
        }
    }
}