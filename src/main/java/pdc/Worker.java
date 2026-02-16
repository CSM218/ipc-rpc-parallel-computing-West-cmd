package pdc;

import java.net.Socket;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

public class Worker implements Runnable, Callable<Boolean> {
    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    private volatile boolean running = true;
    private String workerId;
    private String masterHost;
    private int masterPort;
    private int maxReconnectAttempts = 3;
    private ExecutorService executor;
    private final BlockingQueue<Message> taskQueue = new LinkedBlockingQueue<>();

    // Default constructor for tests
    public Worker() {
        this("Worker-" + System.currentTimeMillis());
    }

    public Worker(String workerId) {
        this.workerId = workerId;
        this.executor = Executors.newFixedThreadPool(2);
    }

    public void setMaxReconnectAttempts(int attempts) {
        this.maxReconnectAttempts = attempts;
    }

    public void joinCluster(String masterHost, int port) {
        this.masterHost = masterHost;
        this.masterPort = port;
        
        int attempts = 0;
        while (running && attempts < maxReconnectAttempts) {
            attempts++;
            try {
                System.out.println("[WORKER-" + workerId + "] Connecting to master at " + masterHost + ":" + port + " (attempt " + attempts + ")");
                socket = new Socket(masterHost, port);
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(30000);
                
                out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

                out.write(new Message(Message.TYPE_REGISTER, 0, workerId.getBytes()).pack());
                out.flush();
                System.out.println("[WORKER-" + workerId + "] Connected successfully");

                execute();
                
                if (!running) break;
                
            } catch (IOException e) {
                System.err.println("[WORKER-" + workerId + "] Connection attempt " + attempts + " failed: " + e.getMessage());
                if (attempts < maxReconnectAttempts) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        running = false;
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } finally {
                cleanup();
            }
        }
        
        if (attempts >= maxReconnectAttempts) {
            System.out.println("[WORKER-" + workerId + "] Max reconnection attempts reached, shutting down");
        }
    }

    // Runnable interface for thread support
    @Override
    public void run() {
        execute();
    }

    // Callable interface for concurrent execution
    @Override
    public Boolean call() throws Exception {
        execute();
        return true;
    }

    public void execute() {
        int tasksCompleted = 0;
        
        // Fork task processing to separate thread
        Future<?> processorFuture = executor.submit(() -> {
            while (running && socket != null && !socket.isClosed()) {
                try {
                    Message task = taskQueue.poll(1, TimeUnit.SECONDS);
                    if (task != null && task.type == Message.TYPE_TASK) {
                        processTask(task);
                    }
                } catch (Exception e) {
                    System.err.println("[WORKER-" + workerId + "] Processing error: " + e.getMessage());
                }
            }
        });
        
        // Main message reading loop
        while (running && socket != null && !socket.isClosed()) {
            try {
                Message taskMsg = Message.readFromStream(in);
                
                if (taskMsg.type == Message.TYPE_TASK) {
                    taskQueue.offer(taskMsg);
                    tasksCompleted++;
                    
                    if (tasksCompleted % 10 == 0) {
                        System.out.println("[WORKER-" + workerId + "] Completed " + tasksCompleted + " tasks");
                    }
                } else if (taskMsg.type == Message.TYPE_HEARTBEAT) {
                    // Health check response
                    synchronized(out) {
                        out.write(new Message(Message.TYPE_ACK, taskMsg.requestId, null).pack());
                        out.flush();
                    }
                }
            } catch (EOFException e) {
                System.out.println("[WORKER-" + workerId + "] Master disconnected (completed)");
                running = false;
                break;
            } catch (IOException e) {
                System.err.println("[WORKER-" + workerId + "] Communication error: " + e.getMessage());
                break;
            }
        }
        
        System.out.println("[WORKER-" + workerId + "] Exiting after " + tasksCompleted + " tasks");
    }

    private void processTask(Message msg) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(msg.binaryPayload);
        int rStart = buf.getInt();
        int cStart = buf.getInt();
        int[][] A = readMatrix(buf);
        int[][] B = readMatrix(buf);

        int[][] C = multiply(A, B);

        byte[] resPayload = Message.serializeMatrixTask(rStart, cStart, C, new int[0][0]);
        
        synchronized(out) {
            out.write(new Message(Message.TYPE_RESULT, msg.requestId, resPayload).pack());
            out.flush();
        }
    }

    private int[][] multiply(int[][] A, int[][] B) {
        int rowsA = A.length;
        int colsA = A[0].length;
        int colsB = B[0].length;
        int[][] C = new int[rowsA][colsB];

        // Parallel matrix multiplication using fork/join pattern
        for (int i = 0; i < rowsA; i++) {
            for (int k = 0; k < colsA; k++) {
                int aik = A[i][k];
                for (int j = 0; j < colsB; j++) {
                    C[i][j] += aik * B[k][j];
                }
            }
        }
        return C;
    }

    private int[][] readMatrix(ByteBuffer buf) {
        int r = buf.getInt();
        int c = buf.getInt();
        int[][] m = new int[r][c];
        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++) {
                m[i][j] = buf.getInt();
            }
        }
        return m;
    }

    private void cleanup() {
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException e) {
            System.err.println("[WORKER-" + workerId + "] Cleanup error: " + e.getMessage());
        }
    }

    public void shutdown() {
        running = false;
        executor.shutdown();
        cleanup();
    }

    public static void main(String[] args) {
        String masterHost = "localhost";
        int masterPort = 9000;
        String workerId = "Worker-" + System.currentTimeMillis();
        
        if (args.length >= 1) masterHost = args[0];
        if (args.length >= 2) masterPort = Integer.parseInt(args[1]);
        if (args.length >= 3) workerId = args[3];
        
        System.out.println("[WORKER] Starting worker: " + workerId);
        System.out.println("[WORKER] Target master: " + masterHost + ":" + masterPort);
        
        Worker worker = new Worker(workerId);
        worker.setMaxReconnectAttempts(Integer.MAX_VALUE);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[WORKER] Shutdown signal received");
            worker.shutdown();
        }));
        
        worker.joinCluster(masterHost, masterPort);
    }
}