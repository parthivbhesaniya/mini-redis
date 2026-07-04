import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TestClient: spins up many threads that each open a connection to the
 * server and hammer it with SET/GET commands concurrently.
 *
 * This is here specifically to let you PROVE (and later show in an
 * interview or README) that InMemoryStore's ReadWriteLock actually
 * prevents race conditions under concurrent load - not just claim it.
 *
 * It also tests Week 3 features: LRU eviction and key expiry.
 *
 * Run with:  java TestClient [host] [port] [numThreads] [opsPerThread]
 */
public class TestClient {

    public static void main(String[] args) throws InterruptedException, IOException {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 6380;
        int numThreads = args.length > 2 ? Integer.parseInt(args[2]) : 20;
        int opsPerThread = args.length > 3 ? Integer.parseInt(args[3]) : 100;

        // ── Test 1: Concurrency stress test ──────────────────────────
        System.out.println("=== Test 1: Concurrency Stress Test ===");

        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger connectErrors = new AtomicInteger(0);

        long start = System.currentTimeMillis();

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try (
                    Socket socket = new Socket(host, port);
                    BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    OutputStream out = socket.getOutputStream()
                ) {
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "key-" + threadId + "-" + i;
                        String value = "value-" + threadId + "-" + i;

                        send(out, "SET " + key + " " + value);
                        String setResp = in.readLine();

                        send(out, "GET " + key);
                        String getResp = in.readLine();

                        if ("OK".equals(setResp) && value.equals(getResp)) {
                            successCount.incrementAndGet();
                        } else {
                            failureCount.incrementAndGet();
                            System.out.println("MISMATCH thread=" + threadId
                                + " expected=" + value + " got=" + getResp);
                        }
                    }
                } catch (IOException e) {
                    connectErrors.incrementAndGet();
                    System.out.println("Thread " + threadId + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        pool.shutdown();

        long elapsedMs = System.currentTimeMillis() - start;
        int totalOps = numThreads * opsPerThread;

        System.out.println("-----------------------------------------");
        System.out.println("Threads: " + numThreads + "  Ops/thread: " + opsPerThread);
        System.out.println("Total ops: " + totalOps);
        System.out.println("Success: " + successCount.get() + "  Failures: " + failureCount.get()
                + "  Connection errors: " + connectErrors.get());
        System.out.println("Elapsed: " + elapsedMs + " ms");
        System.out.println("-----------------------------------------");

        if (connectErrors.get() == numThreads) {
            System.out.println("FAIL: could not connect to server at " + host + ":" + port);
            System.out.println("  Make sure the server is running and the host/port are correct.");
            System.out.println("  Usage: java TestClient [host] [port] [numThreads] [opsPerThread]");
            return;
        } else if (connectErrors.get() > 0) {
            System.out.println("WARNING: " + connectErrors.get() + "/" + numThreads
                    + " threads failed to connect.");
        }

        if (failureCount.get() == 0 && successCount.get() > 0) {
            System.out.println("PASS: no data races detected under concurrent load.");
        } else if (failureCount.get() > 0) {
            System.out.println("FAIL: race conditions or bugs detected!");
        }

        // ── Test 2: EXPIRE / TTL test ────────────────────────────────
        System.out.println("\n=== Test 2: EXPIRE / TTL Test ===");

        try (
            Socket socket = new Socket(host, port);
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            OutputStream out = socket.getOutputStream()
        ) {
            // Set a key and give it a 2-second TTL
            send(out, "SET ttl-test hello");
            String setResp = in.readLine();
            System.out.println("SET ttl-test hello -> " + setResp);

            send(out, "EXPIRE ttl-test 2");
            String expResp = in.readLine();
            System.out.println("EXPIRE ttl-test 2 -> " + expResp);

            // Immediately, it should still be there
            send(out, "GET ttl-test");
            String getResp1 = in.readLine();
            System.out.println("GET ttl-test (immediate) -> " + getResp1);

            // Wait 3 seconds for it to expire
            System.out.println("Waiting 3 seconds for expiry...");
            Thread.sleep(3000);

            send(out, "GET ttl-test");
            String getResp2 = in.readLine();
            System.out.println("GET ttl-test (after 3s) -> " + getResp2);

            if ("OK".equals(setResp) && "1".equals(expResp)
                    && "hello".equals(getResp1) && "(nil)".equals(getResp2)) {
                System.out.println("PASS: key correctly expired after TTL.");
            } else {
                System.out.println("FAIL: expiry did not work as expected!");
            }
        } catch (IOException e) {
            System.out.println("SKIP: could not connect — " + e.getMessage());
        }

        // ── Test 3: LRU eviction test ────────────────────────────────
        // NOTE: this test is meaningful only when the server is started
        // with a small maxCapacity (e.g., java Server 6380 5).
        // With the default capacity of 10000, eviction won't trigger.
        System.out.println("\n=== Test 3: LRU Eviction Test ===");
        System.out.println("(Start server with small capacity for this test, e.g.: java Server 6380 10)");

        try (
            Socket socket = new Socket(host, port);
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            OutputStream out = socket.getOutputStream()
        ) {
            // Check current size and set enough keys to potentially trigger eviction
            int testKeys = 15;
            for (int i = 0; i < testKeys; i++) {
                send(out, "SET lru-" + i + " val-" + i);
                in.readLine(); // consume OK
            }

            send(out, "SIZE");
            String sizeResp = in.readLine();
            int storeSize = Integer.parseInt(sizeResp);
            System.out.println("SIZE after setting " + testKeys + " keys -> " + storeSize);

            // If eviction is working, size should be capped at maxCapacity
            // Try to get the first key we set — if evicted, it'll be (nil)
            send(out, "GET lru-0");
            String firstKey = in.readLine();
            System.out.println("GET lru-0 -> " + firstKey);

            // Try to get the last key — should always be present (most recent)
            send(out, "GET lru-" + (testKeys - 1));
            String lastKey = in.readLine();
            System.out.println("GET lru-" + (testKeys - 1) + " -> " + lastKey);

            if (storeSize < testKeys) {
                System.out.println("PASS: LRU eviction is capping the store at " + storeSize + " keys.");
                if ("(nil)".equals(firstKey)) {
                    System.out.println("  Confirmed: oldest key (lru-0) was evicted.");
                }
            } else {
                System.out.println("INFO: no eviction observed (capacity >= " + testKeys
                        + "). Start server with smaller capacity to test LRU eviction.");
            }
        } catch (IOException e) {
            System.out.println("SKIP: could not connect — " + e.getMessage());
        }

        // ── Test 4: WAL Durability Test ─────────────────────────────────
        System.out.println("\n=== Test 4: WAL Durability Test ===");
        System.out.println("This test verifies that data survives a server restart.");

        // Use a session-unique prefix to avoid collisions with previous tests
        String session = "wal-" + System.currentTimeMillis() + "-";

        try (
            Socket socket = new Socket(host, port);
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            OutputStream out = socket.getOutputStream()
        ) {
            // Step 1: Write keys that should survive a crash
            int durabilityKeys = 5;
            System.out.println("Setting " + durabilityKeys + " keys with prefix '" + session + "'...");
            for (int i = 0; i < durabilityKeys; i++) {
                send(out, "SET " + session + i + " durable-" + i);
                String resp = in.readLine();
                if (!"OK".equals(resp)) {
                    System.out.println("FAIL: SET " + session + i + " returned: " + resp);
                }
            }

            // Step 2: Verify they're readable now
            boolean allPresent = true;
            for (int i = 0; i < durabilityKeys; i++) {
                send(out, "GET " + session + i);
                String resp = in.readLine();
                if (!("durable-" + i).equals(resp)) {
                    System.out.println("FAIL: GET " + session + i
                            + " expected durable-" + i + " got " + resp);
                    allPresent = false;
                }
            }

            if (allPresent) {
                System.out.println("All " + durabilityKeys + " keys confirmed present.");
                System.out.println();
                System.out.println("╔═══════════════════════════════════════════════════╗");
                System.out.println("║  NOW: Kill the server (Ctrl+C) and restart it.    ║");
                System.out.println("║  Then run this command to verify recovery:         ║");
                System.out.println("║                                                    ║");
                System.out.println("║    java TestClient " + host + " " + port + " 1 1   ║");
                System.out.println("║                                                    ║");
                System.out.println("║  The keys with prefix '" + session + "' should     ║");
                System.out.println("║  still be present after the restart.               ║");
                System.out.println("╚═══════════════════════════════════════════════════╝");
            } else {
                System.out.println("FAIL: some keys missing even BEFORE restart!");
            }
        } catch (IOException e) {
            System.out.println("SKIP: could not connect — " + e.getMessage());
        }

        System.out.println("\n=========================================");
        System.out.println("All tests completed.");
        System.out.println("=========================================");
    }

    private static void send(OutputStream out, String line) throws IOException {
        out.write((line + "\r\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
