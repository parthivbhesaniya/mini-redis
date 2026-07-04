import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Server: entry point. Opens a raw TCP ServerSocket (no framework - this
 * is deliberate, the point of this project is to demonstrate networking
 * and OS fundamentals directly) and dispatches each accepted connection
 * to a fixed-size thread pool.
 *
 * Week 4: Write-ahead log integration
 * ─────────────────────────────────────
 * On startup the server now:
 *   1. Creates the WriteAheadLog (and the data/ directory if needed).
 *   2. Replays any existing snapshot + WAL entries to rebuild state.
 *   3. Starts the SnapshotDaemon for periodic WAL compaction.
 *   4. On shutdown, triggers a final snapshot and closes the WAL.
 *
 * Run with:  java Server [port] [maxCapacity] [snapshotIntervalSec]
 *   port                 — TCP port to listen on (default 6380)
 *   maxCapacity          — max keys before LRU eviction kicks in (default 10000)
 *   snapshotIntervalSec  — seconds between automatic snapshots (default 300)
 */
public class Server {

    private static final int DEFAULT_PORT = 6380;
    private static final int DEFAULT_MAX_CAPACITY = 10000;
    private static final int DEFAULT_SNAPSHOT_INTERVAL_SEC = 300;
    private static final int THREAD_POOL_SIZE = 50; // bounded, unlike Thread-per-connection unbounded
    private static final long EXPIRY_SWEEP_INTERVAL_MS = 1000; // sweep every second

    public static void main(String[] args) {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        int maxCapacity = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_MAX_CAPACITY;
        int snapshotIntervalSec = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_SNAPSHOT_INTERVAL_SEC;

        LRUStore store = new LRUStore(maxCapacity);

        // ── Write-ahead log: replay existing state, then start logging ──
        WriteAheadLog wal;
        try {
            wal = new WriteAheadLog(".");
        } catch (IOException e) {
            System.err.println("FATAL: could not initialize WAL: " + e.getMessage());
            return;
        }

        // Replay snapshot + WAL to rebuild state from last run
        System.out.println("Recovering state from WAL...");
        int replayed = wal.replay(store);
        if (replayed > 0) {
            System.out.println("Replayed " + replayed + " command(s) — "
                    + store.size() + " key(s) recovered.");
        } else {
            System.out.println("No prior state found — starting fresh.");
        }

        // Pass the WAL to CommandProcessor so mutations are logged
        CommandProcessor commandProcessor = new CommandProcessor(store, wal);
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // Start the background expiry daemon
        Thread expiryThread = ExpiryDaemon.start(store, EXPIRY_SWEEP_INTERVAL_MS);

        // Start the background snapshot daemon
        long snapshotIntervalMs = snapshotIntervalSec * 1000L;
        Thread snapshotThread = SnapshotDaemon.start(wal, store, snapshotIntervalMs);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("mini-redis listening on port " + port);
            System.out.println("Max capacity: " + maxCapacity + " keys (LRU eviction enabled)");
            System.out.println("WAL: data/wal.log | Snapshots: every "
                    + snapshotIntervalSec + "s to data/snapshot.rdb");
            System.out.println("Try: telnet localhost " + port + "  or run TestClient");

            // Graceful shutdown: snapshot + close WAL + shutdown pool
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down...");
                System.out.println("Taking final snapshot before exit...");
                wal.snapshot(store);
                wal.close();
                pool.shutdown();
                expiryThread.interrupt();
                snapshotThread.interrupt();
                System.out.println("Goodbye.");
            }));

            while (true) {
                Socket clientSocket = serverSocket.accept(); // blocks until a client connects
                pool.submit(new ClientHandler(clientSocket, commandProcessor));
            }
        } catch (IOException e) {
            System.err.println("Server failed to start: " + e.getMessage());
        }
    }
}
