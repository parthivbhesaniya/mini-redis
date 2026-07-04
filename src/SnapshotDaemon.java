/**
 * SnapshotDaemon: a background thread that periodically triggers a
 * point-in-time snapshot of the store, compacting the WAL.
 *
 * DBMS concept: Checkpointing
 * ────────────────────────────
 * Without periodic snapshots the WAL would grow forever — every SET, DEL,
 * and EXPIRE since the server first started. Replaying a huge log on
 * startup would be slow. Snapshots are the solution: dump the current
 * state to a compact file, then truncate the log.
 *
 * This is the same idea as:
 *   - Postgres checkpoints (write dirty pages, advance the WAL start)
 *   - Redis RDB snapshots (fork + dump)
 *   - SQLite checkpointing (copy WAL pages to the main DB)
 *
 * Like ExpiryDaemon, this runs as a daemon thread so it won't prevent
 * JVM shutdown.
 */
public class SnapshotDaemon implements Runnable {

    private final WriteAheadLog wal;
    private final Store store;
    private final long intervalMs;

    /**
     * @param wal         the WAL to snapshot
     * @param store       the store whose state to dump
     * @param intervalMs  how often to snapshot (in milliseconds)
     */
    public SnapshotDaemon(WriteAheadLog wal, Store store, long intervalMs) {
        this.wal = wal;
        this.store = store;
        this.intervalMs = intervalMs;
    }

    @Override
    public void run() {
        System.out.println("[SnapshotDaemon] Started — snapshotting every "
                + (intervalMs / 1000) + " seconds");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                // Shutdown requested — exit cleanly
                Thread.currentThread().interrupt();
                break;
            }

            int keys = wal.snapshot(store);
            if (keys >= 0) {
                System.out.println("[SnapshotDaemon] Snapshot saved: "
                        + keys + " key(s)");
            }
        }

        System.out.println("[SnapshotDaemon] Stopped.");
    }

    /**
     * Convenience factory: creates and starts the daemon thread.
     * Returns the Thread so the caller can interrupt it if needed.
     */
    public static Thread start(WriteAheadLog wal, Store store, long intervalMs) {
        Thread t = new Thread(new SnapshotDaemon(wal, store, intervalMs));
        t.setName("snapshot-daemon");
        t.setDaemon(true);  // won't prevent JVM shutdown
        t.start();
        return t;
    }
}
