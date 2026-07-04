import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WriteAheadLog (WAL): provides crash-recovery durability for mini-redis.
 *
 * DBMS concept: Write-Ahead Logging
 * ───────────────────────────────────
 * The core principle is simple: before we acknowledge a write to the client,
 * we first persist it to a sequential log file on disk. If the server crashes,
 * we replay the log on restart to reconstruct the exact state we had.
 *
 * This is the same idea behind:
 *   - Postgres's WAL (Write-Ahead Log)
 *   - MySQL's redo log / InnoDB log
 *   - Redis's AOF (Append-Only File)
 *   - SQLite's journal
 *
 * Our format is a simple text protocol (one command per line):
 *   <timestamp_ms> SET key value
 *   <timestamp_ms> DEL key
 *   <timestamp_ms> EXPIRE key seconds
 *
 * Why text and not binary? This is an educational project — being able to
 * `cat data/wal.log` and see exactly what happened is more valuable than
 * the space savings of a binary format.
 *
 * Durability trade-off: flush-per-write
 * ──────────────────────────────────────
 * We call flush() after every append. This guarantees that if the JVM crashes
 * right after we return "OK" to the client, the command is already on disk.
 * The cost is lower write throughput (each write waits for an I/O round-trip).
 * This is equivalent to Redis AOF's `appendfsync always` mode. A production
 * system might batch flushes (like `appendfsync everysec`) for better
 * throughput at the risk of losing up to 1 second of writes on crash.
 *
 * Snapshots
 * ─────────
 * The WAL grows forever if we never compact it. Periodic snapshots dump
 * the entire current state as a series of SET commands into a snapshot file,
 * then truncate the WAL. On startup we load the snapshot first, then replay
 * any WAL entries that came after it.
 *
 * Atomic snapshot writes use temp-file + rename, which is atomic on POSIX
 * filesystems — no half-written snapshots.
 */
public class WriteAheadLog {

    private static final String DATA_DIR = "data";
    private static final String WAL_FILE = "wal.log";
    private static final String SNAPSHOT_FILE = "snapshot.rdb";
    private static final String SNAPSHOT_TEMP = "snapshot.rdb.tmp";

    private final Path dataDir;
    private final Path walPath;
    private final Path snapshotPath;
    private final Path snapshotTempPath;

    private BufferedWriter walWriter;
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * @param baseDir the base directory for data files (typically the working
     *                directory). The WAL and snapshot files will be stored in
     *                a "data/" subdirectory under this path.
     */
    public WriteAheadLog(String baseDir) throws IOException {
        this.dataDir = Paths.get(baseDir, DATA_DIR);
        this.walPath = dataDir.resolve(WAL_FILE);
        this.snapshotPath = dataDir.resolve(SNAPSHOT_FILE);
        this.snapshotTempPath = dataDir.resolve(SNAPSHOT_TEMP);

        // Create data directory if it doesn't exist
        Files.createDirectories(dataDir);

        // Open WAL in append mode — existing entries are preserved
        this.walWriter = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(walPath.toFile(), true), StandardCharsets.UTF_8));
    }

    // ────────── Append (called on every mutating command) ──────────

    /**
     * Log a SET command to the WAL. Called BEFORE the response is sent
     * to the client, ensuring the write is durable.
     */
    public void logSet(String key, String value) {
        append("SET " + key + " " + value);
    }

    /**
     * Log a DEL command to the WAL.
     */
    public void logDelete(String key) {
        append("DEL " + key);
    }

    /**
     * Log an EXPIRE command to the WAL.
     */
    public void logExpire(String key, int seconds) {
        append("EXPIRE " + key + " " + seconds);
    }

    /**
     * Core append: writes a timestamped line to the WAL and flushes.
     *
     * The lock ensures that concurrent client threads don't interleave
     * their writes (which would corrupt the log). Since each append is
     * just one short line + flush, contention will be minimal.
     */
    private void append(String entry) {
        lock.lock();
        try {
            walWriter.write(System.currentTimeMillis() + " " + entry);
            walWriter.newLine();
            walWriter.flush();
        } catch (IOException e) {
            System.err.println("[WAL] ERROR writing to log: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    // ────────── Replay (called once on startup) ──────────

    /**
     * Rebuild state by replaying persisted commands.
     *
     * Order:
     *   1. Load snapshot (if it exists) — bulk SET commands
     *   2. Replay WAL entries on top (commands that occurred after the snapshot)
     *
     * @param store the store to replay commands into
     * @return the total number of commands replayed
     */
    public int replay(Store store) {
        int count = 0;

        // 1. Load snapshot first
        if (Files.exists(snapshotPath)) {
            count += replayFile(snapshotPath, store, "snapshot");
        }

        // 2. Replay WAL on top
        if (Files.exists(walPath)) {
            count += replayFile(walPath, store, "WAL");
        }

        return count;
    }

    /**
     * Replay a single file (either snapshot or WAL) into the store.
     * Each line has the format: <timestamp_ms> <COMMAND> <args...>
     *
     * Corrupted or unrecognized lines are skipped with a warning —
     * partial writes from a crash should not prevent recovery of
     * the rest of the log.
     */
    private int replayFile(Path file, Store store, String label) {
        int count = 0;
        int skipped = 0;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(file.toFile()), StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;

                try {
                    // Format: <timestamp> <COMMAND> <args...>
                    // We split into 4 parts max to preserve spaces in values
                    String[] parts = line.trim().split("\\s+", 4);
                    if (parts.length < 2) {
                        skipped++;
                        continue;
                    }

                    // parts[0] = timestamp, parts[1] = command
                    String command = parts[1].toUpperCase();

                    switch (command) {
                        case "SET":
                            if (parts.length < 4) { skipped++; continue; }
                            store.set(parts[2], parts[3]);
                            count++;
                            break;

                        case "DEL":
                            if (parts.length < 3) { skipped++; continue; }
                            store.delete(parts[2]);
                            count++;
                            break;

                        case "EXPIRE":
                            if (parts.length < 4) { skipped++; continue; }
                            int seconds = Integer.parseInt(parts[3]);
                            store.expire(parts[2], seconds);
                            count++;
                            break;

                        default:
                            skipped++;
                            break;
                    }
                } catch (Exception e) {
                    skipped++;
                }
            }
        } catch (IOException e) {
            System.err.println("[WAL] ERROR reading " + label + ": " + e.getMessage());
        }

        if (skipped > 0) {
            System.out.println("[WAL] Warning: skipped " + skipped
                    + " corrupted/unrecognized line(s) in " + label);
        }

        return count;
    }

    // ────────── Snapshot (called periodically by SnapshotDaemon) ──────────

    /**
     * Create a point-in-time snapshot and truncate the WAL.
     *
     * The process:
     *   1. Dump all current key-value pairs from the store into a temp file
     *      (data/snapshot.rdb.tmp) as timestamped SET commands.
     *   2. Atomically rename temp → snapshot.rdb (POSIX atomic rename).
     *   3. Truncate the WAL (close + reopen in overwrite mode).
     *
     * Why temp-file + rename?
     * If we crash mid-write, the temp file is incomplete but the old
     * snapshot.rdb is still intact. On next startup we load the old
     * (complete) snapshot safely. This is the same pattern Postgres and
     * Redis use for safe file replacement.
     *
     * @param store the store to snapshot
     * @return the number of keys written to the snapshot
     */
    public int snapshot(Store store) {
        lock.lock();
        try {
            // 1. Dump current state to temp file
            Map<String, String> allData = store.getAll();
            long timestamp = System.currentTimeMillis();

            try (BufferedWriter snapWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(snapshotTempPath.toFile()), StandardCharsets.UTF_8))) {

                for (Map.Entry<String, String> entry : allData.entrySet()) {
                    snapWriter.write(timestamp + " SET " + entry.getKey()
                            + " " + entry.getValue());
                    snapWriter.newLine();
                }
                snapWriter.flush();
            }

            // 2. Atomic rename: temp → snapshot
            Files.move(snapshotTempPath, snapshotPath,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);

            // 3. Truncate the WAL (close current writer, reopen in overwrite mode)
            walWriter.close();
            walWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(walPath.toFile(), false), StandardCharsets.UTF_8));

            System.out.println("[WAL] Snapshot complete: " + allData.size()
                    + " keys written, WAL truncated.");
            return allData.size();

        } catch (IOException e) {
            System.err.println("[WAL] ERROR during snapshot: " + e.getMessage());
            return -1;
        } finally {
            lock.unlock();
        }
    }

    // ────────── Lifecycle ──────────

    /**
     * Close the WAL writer cleanly. Called on server shutdown.
     */
    public void close() {
        lock.lock();
        try {
            walWriter.flush();
            walWriter.close();
        } catch (IOException e) {
            System.err.println("[WAL] ERROR closing log: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    /** Expose data directory path for diagnostics. */
    public Path getDataDir() {
        return dataDir;
    }
}
