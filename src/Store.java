/**
 * Store defines the contract for our key-value storage engine.
 *
 * Why an interface?
 * ------------------
 * This is deliberate OOP design: today the implementation is a plain
 * HashMap (InMemoryStore). In Week 3 we'll add an LRU-evicting store.
 * By coding against this interface everywhere else (ClientHandler etc.),
 * we can swap storage strategies without touching any networking code.
 * This is the Strategy Pattern.
 */
public interface Store {

    /**
     * Insert or update a key with a value.
     */
    void set(String key, String value);

    /**
     * Retrieve a value by key.
     * @return the value, or null if the key does not exist.
     */
    String get(String key);

    /**
     * Remove a key from the store.
     * @return true if the key existed and was removed, false otherwise.
     */
    boolean delete(String key);

    /**
     * Number of keys currently stored.
     */
    int size();

    /**
     * Set a time-to-live on an existing key. After `seconds` elapse the
     * key should be treated as deleted.
     *
     * @return true if the key exists and the TTL was set, false otherwise.
     */
    boolean expire(String key, int seconds);

    /**
     * Return a snapshot of all keys currently in the store.
     * Useful for the KEYS command and for diagnostics.
     */
    java.util.List<String> keys();

    /**
     * Return a snapshot of all key-value pairs currently in the store.
     * Used by the WAL snapshot writer to dump the entire state to disk.
     *
     * @return a new Map containing all non-expired key-value pairs.
     */
    java.util.Map<String, String> getAll();
}
