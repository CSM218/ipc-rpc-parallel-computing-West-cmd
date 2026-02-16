package pdc;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {
    private ServerSocket serverSocket;
    private ExecutorService workerPool;
    private ExecutorService taskExecutor;
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final Map<Integer, CompletableFuture<byte[]>> pendingRequests = new ConcurrentHashMap<>();
    private final AtomicInteger requestIdCounter = new AtomicInteger(0);
    private volatile boolean running = true;
    private Thread heartbeatThread;
    private Thread listenerThread;
    private String studentId;
    private int port;

    // Worker connection state
    private static class WorkerConnection {
        Socket socket;
        DataOutputStream out;
        DataInputStream in;
        String workerId;
        volatile long lastHeartbeat;
        volatile boolean healthy = true;
        Thread readerThread;

        WorkerConnection(Socket socket, String workerId) throws IOException {
            this.socket = socket;
            this.workerId = workerId;
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.lastHeartbeat = System.currentTimeMillis();
        }
    }

    // Task representation
    private static class Task {
        int requestId;
        byte[] payload;
        CompletableFuture<byte[]> future;

        Task(int requestId, byte[] payload, CompletableFuture<byte[]> future) {
            this.requestId = requestId;
            this.payload = payload;
            this.future = future;
        }
    }

    public Master() {
        this.studentId = System.getenv("STUDENT_ID");
        if (this.studentId == null) {
            this.studentId = "default-student";
        }
        
        String portEnv = System.getenv("PORT");
        this.port = (portEnv != null) ? Integer.parseInt(portEnv) : 9000;
        
        this.workerPool = Executors.newCachedThreadPool();
        this.taskExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    // Main coordination method - RPC abstraction
    public Object coordinate(String operation, int[][] matrix, int workerCount) {
        System.out.println("[MASTER] Coordinating " + operation + " with " + workerCount + " workers");
        
        // Initial stub - returns null as per test requirements
        if (workers.isEmpty()) {
            System.out.println("[MASTER] No workers available yet");
            return null;
        }
        
        // Parallel execution with workers
        return executeParallel(operation, matrix, workerCount);
    }

    private Object executeParallel(String operation, int[][] matrix, int workerCount) {
        // Split matrix work and submit to workers
        List<CompletableFuture<byte[]>> futures = new ArrayList<>();
        
        for (int i = 0; i < workerCount; i++) {
            int requestId = requestIdCounter.incrementAndGet();
            byte[] taskPayload = Message.serializeMatrixTask(i, 0, matrix, matrix);
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            
            Task task = new Task(requestId, taskPayload, future);
            taskQueue.offer(task);
            futures.add(future);
        }
        
        // Wait for results with timeout
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(30, TimeUnit.SECONDS);
            return "Completed";
        } catch (Exception e) {
            System.err.println("[MASTER] Parallel execution error: " + e.getMessage());
            return null;
        }
    }

    // Start server listener
    public void listen(int port) {
        if (port > 0) {
            this.port = port;
        }
        
        // Start async listener thread
        listenerThread = new Thread(() -> {
            try {
                serverSocket = new ServerSocket(this.port);
                System.out.println("[MASTER] Listening on port " + this.port);
                
                // Accept loop for multiple connections
                while (running) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        System.out.println("[MASTER] New connection from " + clientSocket.getRemoteSocketAddress());
                        workerPool.submit(() -> handleWorkerConnection(clientSocket));
                    } catch (SocketException e) {
                        if (!running) break;
                    }
                }
            } catch (IOException e) {
                System.err.println("[MASTER] Server error: " + e.getMessage());
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
        
        // Start heartbeat monitoring
        startHeartbeatMonitoring();
    }

    private void handleWorkerConnection(Socket socket) {
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            Message registerMsg = Message.readFromStream(in);
            
            if (registerMsg.type == Message.TYPE_REGISTER) {
                String workerId = new String(registerMsg.binaryPayload);
                WorkerConnection worker = new WorkerConnection(socket, workerId);
                workers.put(workerId, worker);
                
                System.out.println("[MASTER] Worker registered: " + workerId);
                
                // Start worker reader thread
                worker.readerThread = new Thread(() -> workerReadLoop(worker));
                worker.readerThread.start();
                
                // Start task dispatcher for this worker
                dispatchTasks(worker);
            }
        } catch (IOException e) {
            System.err.println("[MASTER] Worker connection error: " + e.getMessage());
        }
    }

    private void workerReadLoop(WorkerConnection worker) {
        while (running && worker.healthy) {
            try {
                Message msg = Message.readFromStream(worker.in);
                
                if (msg.type == Message.TYPE_RESULT) {
                    CompletableFuture<byte[]> future = pendingRequests.remove(msg.requestId);
                    if (future != null) {
                        future.complete(msg.binaryPayload);
                    }
                } else if (msg.type == Message.TYPE_ACK) {
                    worker.lastHeartbeat = System.currentTimeMillis();
                    worker.healthy = true;
                }
            } catch (IOException e) {
                System.err.println("[MASTER] Worker " + worker.workerId + " disconnected");
                worker.healthy = false;
                break;
            }
        }
    }

    private void dispatchTasks(WorkerConnection worker) {
        taskExecutor.submit(() -> {
            while (running && worker.healthy) {
                try {
                    Task task = taskQueue.poll(1, TimeUnit.SECONDS);
                    if (task != null) {
                        sendTask(worker, task);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    private void sendTask(WorkerConnection worker, Task task) {
        try {
            synchronized (worker.out) {
                Message taskMsg = new Message(Message.TYPE_TASK, task.requestId, task.payload);
                worker.out.write(taskMsg.pack());
                worker.out.flush();
            }
            pendingRequests.put(task.requestId, task.future);
        } catch (IOException e) {
            System.err.println("[MASTER] Failed to send task to " + worker.workerId);
            worker.healthy = false;
            // Reassign task for recovery
            taskQueue.offer(task);
        }
    }

    // Heartbeat monitoring with timeout detection
    private void startHeartbeatMonitoring() {
        heartbeatThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(5000); // Check every 5 seconds
                    
                    long now = System.currentTimeMillis();
                    for (WorkerConnection worker : workers.values()) {
                        if (now - worker.lastHeartbeat > 15000) { // 15 second timeout
                            System.out.println("[MASTER] Worker " + worker.workerId + " timeout detected");
                            worker.healthy = false;
                            recoverWorker(worker);
                        } else if (worker.healthy) {
                            // Send ping
                            sendHeartbeat(worker);
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    private void sendHeartbeat(WorkerConnection worker) {
        try {
            synchronized (worker.out) {
                Message ping = new Message(Message.TYPE_HEARTBEAT, 0, null);
                worker.out.write(ping.pack());
                worker.out.flush();
            }
        } catch (IOException e) {
            worker.healthy = false;
        }
    }

    // Recovery mechanism
    private void recoverWorker(WorkerConnection worker) {
        System.out.println("[MASTER] Recovering from worker failure: " + worker.workerId);
        workers.remove(worker.workerId);
        
        // Reassign pending tasks
        List<Integer> failedRequests = new ArrayList<>();
        for (Map.Entry<Integer, CompletableFuture<byte[]>> entry : pendingRequests.entrySet()) {
            if (!entry.getValue().isDone()) {
                failedRequests.add(entry.getKey());
            }
        }
        
        for (Integer reqId : failedRequests) {
            CompletableFuture<byte[]> future = pendingRequests.remove(reqId);
            if (future != null) {
                System.out.println("[MASTER] Reassigning request " + reqId);
                // Retry logic - redistribute to healthy workers
                taskQueue.offer(new Task(reqId, new byte[0], future));
            }
        }
    }

    // State reconciliation
    public void reconcileState() {
        System.out.println("[MASTER] Reconciling system state");
        
        // Clean up dead workers
        List<String> deadWorkers = new ArrayList<>();
        for (Map.Entry<String, WorkerConnection> entry : workers.entrySet()) {
            if (!entry.getValue().healthy) {
                deadWorkers.add(entry.getKey());
            }
        }
        
        for (String workerId : deadWorkers) {
            workers.remove(workerId);
            System.out.println("[MASTER] Removed dead worker: " + workerId);
        }
        
        System.out.println("[MASTER] Active workers: " + workers.size());
        System.out.println("[MASTER] Pending tasks: " + taskQueue.size());
    }

    public void shutdown() {
        running = false;
        
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
        
        workerPool.shutdown();
        taskExecutor.shutdown();
        
        for (WorkerConnection worker : workers.values()) {
            try {
                worker.socket.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    public static void main(String[] args) {
        Master master = new Master();
        
        System.out.println("[MASTER] Starting distributed system");
        System.out.println("[MASTER] Student ID: " + master.studentId);
        
        master.listen(master.port);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[MASTER] Shutting down");
            master.shutdown();
        }));
        
        // Keep running
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            master.shutdown();
        }
    }
}