/**
 * CommandProcessor: parses a single line of client input and executes it
 * against the Store.
 *
 * Protocol (simplified, inspired by Redis's RESP):
 *   SET key value       -> "OK"
 *   GET key             -> value, or "(nil)" if missing
 *   DEL key             -> "1" if deleted, "0" if key didn't exist
 *   PING                -> "PONG"
 *   SIZE                -> number of keys currently stored
 *   EXPIRE key seconds  -> "1" if TTL was set, "0" if key doesn't exist
 *   KEYS                -> space-separated list of keys, or "(empty)"
 *
 * Keeping parsing separate from the networking code (ClientHandler) and
 * from storage (Store) is a deliberate separation of concerns - each class
 * has exactly one reason to change.
 *
 * Week 4: Write-ahead logging integration
 * ─────────────────────────────────────────
 * Mutating commands (SET, DEL, EXPIRE) are logged to the WAL BEFORE
 * the response is returned to the client. This ensures durability:
 * if the server crashes after we send "OK", the write is already on disk
 * and will be replayed on restart.
 */
public class CommandProcessor {

    private final Store store;
    private final WriteAheadLog wal; // null = no durability (e.g., during replay)

    /**
     * Constructor with WAL support.
     *
     * @param store the storage engine
     * @param wal   the write-ahead log (may be null to disable logging,
     *              e.g., during WAL replay to avoid re-logging replayed commands)
     */
    public CommandProcessor(Store store, WriteAheadLog wal) {
        this.store = store;
        this.wal = wal;
    }

    /**
     * Convenience constructor without WAL (backwards compatible, used
     * during replay or testing without persistence).
     */
    public CommandProcessor(Store store) {
        this(store, null);
    }

    /**
     * Executes a single raw command line and returns the response to send
     * back to the client (without trailing newline).
     */
    public String process(String rawLine) {
        if (rawLine == null || rawLine.isBlank()) {
            return "ERR empty command";
        }

        // Split on whitespace, but keep SET's value intact even if it
        // contains spaces by limiting the split count.
        String[] parts = rawLine.trim().split("\\s+", 3);
        String command = parts[0].toUpperCase();

        switch (command) {
            case "PING":
                return "PONG";

            case "SIZE":
                return String.valueOf(store.size());

            case "SET":
                if (parts.length < 3) {
                    return "ERR usage: SET key value";
                }
                store.set(parts[1], parts[2]);
                if (wal != null) {
                    wal.logSet(parts[1], parts[2]);
                }
                return "OK";

            case "GET":
                if (parts.length < 2) {
                    return "ERR usage: GET key";
                }
                String value = store.get(parts[1]);
                return value == null ? "(nil)" : value;

            case "DEL":
                if (parts.length < 2) {
                    return "ERR usage: DEL key";
                }
                boolean removed = store.delete(parts[1]);
                if (removed && wal != null) {
                    wal.logDelete(parts[1]);
                }
                return removed ? "1" : "0";

            case "EXPIRE":
                if (parts.length < 3) {
                    return "ERR usage: EXPIRE key seconds";
                }
                try {
                    int seconds = Integer.parseInt(parts[2]);
                    if (seconds <= 0) {
                        return "ERR seconds must be positive";
                    }
                    boolean set = store.expire(parts[1], seconds);
                    if (set && wal != null) {
                        wal.logExpire(parts[1], seconds);
                    }
                    return set ? "1" : "0";
                } catch (NumberFormatException e) {
                    return "ERR seconds must be an integer";
                }

            case "KEYS":
                java.util.List<String> keys = store.keys();
                return keys.isEmpty() ? "(empty)" : String.join(" ", keys);

            default:
                return "ERR unknown command '" + command + "'";
        }
    }
}
