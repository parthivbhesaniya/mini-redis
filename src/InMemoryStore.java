import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * InMemoryStore: a simple hash-table backed key-value store.
 *
 * DSA:
 *  - Backed by java.util.HashMap -> average O(1) get/set/delete.
 *
 * OS / Concurrency:
 *  - Multiple client threads (see ClientHandler) can call set()/get()/delete()
 *    at the same time. Without synchronization this would cause race
 *    conditions and could corrupt the HashMap's internal structure.
 *  - We use a ReadWriteLock instead of a single mutex:
 *      - Many threads can hold the READ lock simultaneously (GET is safe
 *        to run concurrently with other GETs).
 *      - Only one thread can hold the WRITE lock at a time, and it excludes
 *        all readers too (SET/DELETE must be exclusive).
 *  - This is a classic trade-off you should be ready to explain in
 *    interviews: a single global lock is simpler but serializes even
 *    read-only traffic, hurting throughput on read-heavy workloads.
 */
public class InMemoryStore implements Store {

    private final Map<String, String> data = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public void set(String key, String value) {
        lock.writeLock().lock();
        try {
            data.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String get(String key) {
        lock.readLock().lock();
        try {
            return data.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean delete(String key) {
        lock.writeLock().lock();
        try {
            return data.remove(key) != null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return data.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * InMemoryStore does not support TTL — this is a no-op that always
     * returns false. Use LRUStore for expiry support.
     */
    @Override
    public boolean expire(String key, int seconds) {
        return false;
    }

    @Override
    public List<String> keys() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(data.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<String, String> getAll() {
        lock.readLock().lock();
        try {
            return new HashMap<>(data);
        } finally {
            lock.readLock().unlock();
        }
    }
}
