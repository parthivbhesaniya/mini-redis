/**
 * ExpiryDaemon: a background thread that periodically scans the LRUStore
 * and purges keys whose TTL has elapsed.
 *
 * OS concept: Daemon threads
 * ──────────────────────────
 * This thread is marked as a daemon (setDaemon(true)), which means the
 * JVM will NOT wait for it to finish before exiting. This is the correct
 * choice for housekeeping threads — we don't want a background sweeper
 * to keep the process alive after the main server thread shuts down.
 *
 * How real systems do it:
 * Redis uses a combination of lazy expiry (check on access) and active
 * expiry (periodic random sampling of keys with TTLs). Our approach is
 * simpler — we do a full sweep — but the principle is the same. For a
 * small in-memory dataset this is perfectly fine. In a production system
 * with millions of keys you'd sample instead of scanning.
 */
public class ExpiryDaemon implements Runnable {

    private final LRUStore store;
    private final long sweepIntervalMs;

    /**
     * @param store            the LRUStore to sweep
     * @param sweepIntervalMs  how often to run (in milliseconds)
     */
    public ExpiryDaemon(LRUStore store, long sweepIntervalMs) {
        this.store = store;
        this.sweepIntervalMs = sweepIntervalMs;
    }

    @Override
    public void run() {
        System.out.println("[ExpiryDaemon] Started — sweeping every "
                + sweepIntervalMs + " ms");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(sweepIntervalMs);
            } catch (InterruptedException e) {
                // Shutdown requested — exit cleanly
                Thread.currentThread().interrupt();
                break;
            }

            int purged = store.purgeExpired();
            if (purged > 0) {
                System.out.println("[ExpiryDaemon] Purged " + purged
                        + " expired key(s)");
            }
        }

        System.out.println("[ExpiryDaemon] Stopped.");
    }

    /**
     * Convenience factory: creates and starts the daemon thread.
     * Returns the Thread so the caller can interrupt it if needed.
     */
    public static Thread start(LRUStore store, long sweepIntervalMs) {
        Thread t = new Thread(new ExpiryDaemon(store, sweepIntervalMs));
        t.setName("expiry-daemon");
        t.setDaemon(true);  // won't prevent JVM shutdown
        t.start();
        return t;
    }
}
