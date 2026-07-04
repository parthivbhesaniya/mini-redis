import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LRUStore: a key-value store with Least-Recently-Used eviction and
 * optional per-key time-to-live (TTL).
 *
 * DSA (the core interview-worthy part):
 * ──────────────────────────────────────
 * We combine two data structures:
 *
 *   1. A HashMap<String, Node>  →  O(1) lookup by key
 *   2. A hand-rolled doubly linked list  →  O(1) insertion, removal,
 *      and move-to-head
 *
 * Together they give O(1) for get(), set(), and delete() while
 * maintaining access order so eviction of the LRU entry is also O(1).
 *
 * Why not just use LinkedHashMap?
 * Java's LinkedHashMap can be configured for access-order and has a
 * removeEldestEntry hook. But building it from scratch shows you
 * understand the underlying mechanism — exactly what an interviewer
 * wants to see. It's also more flexible for adding per-node TTLs.
 *
 * Concurrency:
 * ─────────────
 * Same ReadWriteLock strategy as InMemoryStore. Reads (get) can run
 * concurrently; writes (set, delete, evict) are exclusive.
 *
 * Note: get() acquires the WRITE lock because it mutates the list
 * (moves the accessed node to the head). This is a deliberate
 * trade-off: true LRU ordering requires mutation on every read.
 */
public class LRUStore implements Store {

    // ────────── Inner Node class (doubly linked list node) ──────────

    /**
     * Each Node lives in both the HashMap and the doubly linked list.
     * Storing the key inside the node lets us remove the HashMap entry
     * during eviction (we only have a pointer to the tail node, and we
     * need its key to call map.remove()).
     */
    static class Node {
        String key;
        String value;
        Node prev;
        Node next;
        long expireAtMillis; // -1 means "no expiry"

        Node(String key, String value) {
            this.key = key;
            this.value = value;
            this.expireAtMillis = -1;
        }

        boolean isExpired() {
            return expireAtMillis > 0 && System.currentTimeMillis() > expireAtMillis;
        }
    }

    // ────────── Sentinel nodes ──────────
    // Using dummy head/tail sentinels eliminates null-checks in every
    // list operation — a clean trick worth mentioning in interviews.

    private final Node head; // dummy — head.next is the MRU node
    private final Node tail; // dummy — tail.prev is the LRU node

    private final Map<String, Node> map;
    private final int maxCapacity;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LRUStore(int maxCapacity) {
        if (maxCapacity <= 0) {
            throw new IllegalArgumentException("maxCapacity must be > 0");
        }
        this.maxCapacity = maxCapacity;
        this.map = new HashMap<>();

        // Wire up sentinels
        head = new Node(null, null);
        tail = new Node(null, null);
        head.next = tail;
        tail.prev = head;
    }

    // ────────── Store interface ──────────

    @Override
    public void set(String key, String value) {
        lock.writeLock().lock();
        try {
            Node existing = map.get(key);
            if (existing != null) {
                // Update in place and promote to MRU
                existing.value = value;
                existing.expireAtMillis = -1; // SET resets any TTL
                unlinkNode(existing);
                addToHead(existing);
            } else {
                // Evict LRU if at capacity
                if (map.size() >= maxCapacity) {
                    evictLRU();
                }
                Node newNode = new Node(key, value);
                map.put(key, newNode);
                addToHead(newNode);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get acquires the WRITE lock because it mutates the linked list
     * (promotes the accessed node to MRU position). This is a conscious
     * trade-off: we sacrifice read concurrency for correct LRU ordering.
     */
    @Override
    public String get(String key) {
        lock.writeLock().lock();
        try {
            Node node = map.get(key);
            if (node == null) {
                return null;
            }
            // Lazy expiry check — if expired, treat as missing
            if (node.isExpired()) {
                removeNode(node);
                return null;
            }
            // Promote to MRU
            unlinkNode(node);
            addToHead(node);
            return node.value;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean delete(String key) {
        lock.writeLock().lock();
        try {
            Node node = map.get(key);
            if (node == null) {
                return false;
            }
            removeNode(node);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return map.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean expire(String key, int seconds) {
        lock.writeLock().lock();
        try {
            Node node = map.get(key);
            if (node == null || node.isExpired()) {
                return false;
            }
            node.expireAtMillis = System.currentTimeMillis() + (seconds * 1000L);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<String> keys() {
        lock.readLock().lock();
        try {
            List<String> result = new ArrayList<>();
            for (Map.Entry<String, Node> entry : map.entrySet()) {
                if (!entry.getValue().isExpired()) {
                    result.add(entry.getKey());
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<String, String> getAll() {
        lock.readLock().lock();
        try {
            Map<String, String> result = new HashMap<>();
            for (Map.Entry<String, Node> entry : map.entrySet()) {
                if (!entry.getValue().isExpired()) {
                    result.put(entry.getKey(), entry.getValue().value);
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    // ────────── Expiry sweep (called by ExpiryDaemon) ──────────

    /**
     * Walk the entire list and remove expired nodes.
     * Called periodically by ExpiryDaemon. This is the "active expiry"
     * strategy — complementing the "lazy expiry" in get().
     *
     * Why both? Lazy expiry alone would leak memory for keys that are
     * set-and-forgotten (never read again). Active sweep ensures they
     * are cleaned up within a bounded time.
     *
     * @return the number of keys purged in this sweep.
     */
    public int purgeExpired() {
        lock.writeLock().lock();
        try {
            int purged = 0;
            Node current = head.next;
            while (current != tail) {
                Node next = current.next; // grab before we unlink
                if (current.isExpired()) {
                    removeNode(current);
                    purged++;
                }
                current = next;
            }
            return purged;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ────────── Linked list internals (all O(1)) ──────────

    /**
     * Insert node right after the head sentinel (MRU position).
     *
     *   head <-> node <-> (old head.next)
     */
    private void addToHead(Node node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    /**
     * Unlink a node from wherever it sits in the list.
     * Does NOT remove from the map.
     */
    private void unlinkNode(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    /**
     * Full removal: unlink from list AND remove from map.
     */
    private void removeNode(Node node) {
        unlinkNode(node);
        map.remove(node.key);
    }

    /**
     * Evict the least-recently-used entry (tail.prev).
     */
    private void evictLRU() {
        Node lru = tail.prev;
        if (lru == head) {
            return; // list is empty, nothing to evict
        }
        System.out.println("[LRU] Evicting key: " + lru.key);
        removeNode(lru);
    }

    /** Expose capacity for diagnostics. */
    public int getMaxCapacity() {
        return maxCapacity;
    }
}
