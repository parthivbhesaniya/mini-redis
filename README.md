# mini-redis

A from-scratch, in-memory key-value database and raw TCP server built in Java. This project implements core computer science fundamentals—including low-level networking, thread safety and locking, Least-Recently-Used (LRU) cache eviction, per-key TTL, and Write-Ahead Log (WAL) crash recovery—without relying on any external frameworks.

---

## 🛠️ Tech Stack & Architecture

- **Language:** Java 17+ (Core SE)
- **Networking:** Raw TCP sockets (`java.net.ServerSocket`, `java.net.Socket`)
- **Concurrency & OS:** Bounded Thread Pool (`java.util.concurrent.ExecutorService`), Reentrant Read-Write Locks (`java.util.concurrent.locks.ReentrantReadWriteLock`), Background Daemon Threads.
- **DSA (Storage Engine):** Hand-rolled Doubly Linked List (DLL) + `java.util.HashMap` for $O(1)$ Least-Recently-Used (LRU) operations.
- **DBMS (Durability):** Write-Ahead Logging (WAL) with immediate flush, atomic checkpointing (temp-file writing + atomic rename).

### System Architecture Diagram

```
                             ┌───────────────────────────────┐
                             │           Clients             │
                             └───────────────┬───────────────┘
                                             │ (TCP Protocol)
                                             ▼
                             ┌───────────────────────────────┐
                             │         TcpServer             │
                             │ (Accepts & dispatches to pool)│
                             └───────────────┬───────────────┘
                                             │
                                             ▼
                             ┌───────────────────────────────┐
                             │       ClientHandler           │
                             │ (Thread-per-connection task)  │
                             └───────────────┬───────────────┘
                                             │ (Raw Text Command)
                                             ▼
                             ┌───────────────────────────────┐
                             │       CommandProcessor        │
                             │ (Parses & validates command)  │
                             └───────┬───────────────┬───────┘
                                     │               │
                        (Reads Only) │               │ (Mutating Writes)
                                     ▼               ▼
                                ┌─────────┐   ┌─────────────┐
                                │  Store  │◄──┤    WAL      ├─► [data/wal.log]
                                │ (RAM)   │   │ (Appends)   │
                                └────┬────┘   └─────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
             InMemoryStore                         LRUStore
            (Plain HashMap)                 (Custom DLL + HashMap)
                                                      ▲
                                        ┌─────────────┴─────────────┐
                                        │                           │
                                 [ExpiryDaemon]             [SnapshotDaemon]
                                 (Sweeps TTLs)             (Compacts to RDB)
```

---

## 📚 Core CS Fundamentals Implemented

### 1. Data Structures & Algorithms (DSA)
- **$O(1)$ LRU Cache:** Instead of wrapping Java's `LinkedHashMap`, the `LRUStore` combines a standard `HashMap` (for $O(1)$ key lookups) and a hand-rolled Doubly Linked List (for $O(1)$ updates to the node order).
- **Sentinel Nodes:** The doubly linked list utilizes dummy `head` and `tail` sentinel nodes, completely eliminating null-pointer edge cases during pointer manipulation.
- **Bidirectional Pointers:** Storing the key inside the DLL node allows eviction to find and remove the key from the `HashMap` in $O(1)$ time.

### 2. Object-Oriented Programming (OOP)
- **Strategy Pattern:** The storage engine is abstractly defined by the `Store` interface. Both `InMemoryStore` and `LRUStore` implement this interface, enabling the server to swap storage strategies without changing a single line of networking or parsing code.
- **Separation of Concerns:** Protocol parsing (`CommandProcessor`), connection lifecycle (`ClientHandler`), durability (`WriteAheadLog`), and storage (`Store`) are strictly decoupled.

### 3. Operating Systems & Concurrency (OS)
- **Bounded Concurrency:** Uses a fixed-size `ExecutorService` (50 threads) to handle client connections. This protects system memory and limits scheduling context-switch overhead under high loads, unlike unbounded thread-per-connection models.
- **Read-Write Locking:** Multiple concurrent readers (`GET`, `SIZE`, `KEYS`) are allowed to access the data concurrently. Mutating operations (`SET`, `DEL`, `EXPIRE`) acquire an exclusive write lock.
- **Daemon Threads:** Housekeeping tasks like key sweeps (`ExpiryDaemon`) and periodic snapshots (`SnapshotDaemon`) run on JVM daemon threads, ensuring they automatically shut down when the primary thread exits.

### 4. Database Durability (DBMS)
- **Write-Ahead Logging (WAL):** To prevent data loss from unexpected crashes (e.g., JVM crashes, power outages), mutating writes are appended and flushed to `data/wal.log` before an `OK` response is sent to the client.
- **Crash Recovery:** On restart, the system replays the log files to reconstruct state.
- **Atomic Renames:** Snapshots are written to a temporary file (`snapshot.rdb.tmp`) and then atomically renamed to `snapshot.rdb` using POSIX filesystem operations, ensuring a crash during snapshot generation never corrupts the existing database state.

---

## 🔄 Detailed Working Pipelines

### 1. The Write/Mutation Pipeline (e.g., `SET name gopal`)

```
Client             TcpServer           ClientHandler        CommandProcessor           WAL                Store (RAM)
  │                    │                     │                     │                    │                      │
  │─── SET name ───────>                     │                     │                    │                      │
  │    (TCP Write)     │                     │                     │                    │                      │
  │                    │─── submit task ────>│                     │                    │                      │
  │                    │    to thread pool   │─── process("SET") ─>│                    │                      │
  │                    │                     │                     │─── write to RAM ──>│                      │                      │
  │                    │                     │                     │                    │─── set("name") ─────>│
  │                    │                     │                     │                    │                      │ (Acquires WriteLock)
  │                    │                     │                     │◄── RAM updated ────│                      │
  │                    │                     │                     │                    │                      │
  │                    │                     │                     │─── append & flush ─> [Write to disk]      │
  │                    │                     │                     │    to data/wal.log │                      │
  │                    │                     │                     │◄── disk flushed ───│                      │
  │                    │                     │◄── returns "OK" ────│                    │                      │
  │◄── TCP: "OK" ───────                     │                     │                    │                      │
```

1. **TCP Connection:** The client writes the command `SET name gopal\r\n` to the raw TCP socket.
2. **Dispatching:** `TcpServer` accepts the connection and dispatches the task to the `ClientHandler` thread pool.
3. **Execution & Lock Acquisition:** The thread parser calls `CommandProcessor`, which invokes `Store.set()`. The store acquires the Reentrant Write Lock, updates the in-memory map, moves the associated node to the head of the DLL, and releases the lock.
4. **Write-Ahead Logging:** `CommandProcessor` logs the operation into `WriteAheadLog` with a timestamp. The log writer performs a physical disk `flush()`.
5. **Client Response:** Only after the log is securely written to disk does the server reply to the client with `OK\r\n`.

---

### 2. Startup and Recovery Pipeline

```
          ┌───────────────────────────┐
          │      Server Startup       │
          └─────────────┬─────────────┘
                        │
                        ▼
          ┌───────────────────────────┐
          │  Check data/snapshot.rdb  │
          └─────────────┬─────────────┘
                        │
         ┌──────────────┴──────────────┐
         │                             │ Yes
         ▼                             ▼
┌──────────────────┐        ┌──────────────────────┐
│   Skip Snapshot  │        │ Replay snapshot.rdb  │
└────────┬─────────┘        │ line-by-line into RAM│
         │                  └──────────┬───────────┘
         │                             │
         └──────────────┬──────────────┘
                        │
                        ▼
          ┌───────────────────────────┐
          │    Check data/wal.log     │
          └─────────────┬─────────────┘
                        │
         ┌──────────────┴──────────────┐
         │                             │ Yes
         ▼                             ▼
┌──────────────────┐        ┌──────────────────────┐
│     Skip WAL     │        │ Replay wal.log       │
└────────┬─────────┘        │ line-by-line into RAM│
         │                  └──────────┬───────────┘
         │                             │
         └──────────────┬──────────────┘
                        │
                        ▼
          ┌───────────────────────────┐
          │   Server Ready to Accept  │
          │     Client Connections    │
          └───────────────────────────┘
```

1. **Initialization:** The `Server` starts and instantiates a fresh `LRUStore`.
2. **Snapshot Recovery:** The server checks for `data/snapshot.rdb`. If found, it reads the snapshot file line-by-line, parsing the historical `SET` states directly into memory.
3. **WAL Recovery:** The server checks for `data/wal.log`. It reads and executes all log commands recorded since the last snapshot.
4. **Execution Complete:** The final in-memory database is fully recovered, and the server opens its socket ports to external traffic.

---

### 3. WAL Compaction / Checkpointing Pipeline (The `SnapshotDaemon`)

```
                  ┌───────────────────────────────┐
                  │ SnapshotDaemon Timer Triggers │
                  └───────────────┬───────────────┘
                                  │ (Every 300 seconds)
                                  ▼
                  ┌───────────────────────────────┐
                  │   Acquire WAL WriteLock       │
                  └───────────────┬───────────────┘
                                  │
                                  ▼
                  ┌───────────────────────────────┐
                  │ Dump all current RAM keys to  │
                  │     data/snapshot.rdb.tmp     │
                  └───────────────┬───────────────┘
                                  │
                                  ▼
                  ┌───────────────────────────────┐
                  │   Atomically Rename tmp to    │
                  │      data/snapshot.rdb        │
                  └───────────────┬───────────────┘
                                  │
                                  ▼
                  ┌───────────────────────────────┐
                  │   Truncate data/wal.log to 0  │
                  └───────────────┬───────────────┘
                                  │
                                  ▼
                  ┌───────────────────────────────┐
                  │   Release WAL WriteLock       │
                  └───────────────────────────────┘
```

1. **Trigger:** Every 300 seconds, the background `SnapshotDaemon` awakes.
2. **Dumping State:** The daemon calls `wal.snapshot()`. It locks the WAL, fetches all active keys in the database via `Store.getAll()`, and writes them to `data/snapshot.rdb.tmp` as standard `SET` instructions.
3. **Atomic Swap:** It closes the temp file and renames it to `data/snapshot.rdb` using an atomic copy operation (`Files.move` with `ATOMIC_MOVE`).
4. **Log Truncation:** The daemon truncates `data/wal.log` (overwriting it as an empty file), freeing disk space. It then releases the WAL write lock.

---

## 💬 Protocol Specification

Commands are sent as plain text lines ended with a carriage return and newline (`\r\n`):

| Command Syntax | Description | Successful Response | Failure/Nil Response |
|---|---|---|---|
| **`PING`** | Ping the server | `PONG` | - |
| **`SET <key> <val>`** | Store a key-value pair | `OK` | `ERR ...` |
| **`GET <key>`** | Retrieve the value of a key | `<value>` | `(nil)` |
| **`DEL <key>`** | Delete a key from the database | `1` | `0` (key not found) |
| **`EXPIRE <key> <sec>`**| Set a Time-to-Live (in seconds) | `1` | `0` (key not found) |
| **`SIZE`** | Count active keys | `<number>` | - |
| **`KEYS`** | List all non-expired keys | `<key1> <key2> ...` | `(empty)` |
| **`QUIT`** / **`EXIT`** | Disconnect from server | Closes socket | - |

---

## 🚀 How to Run & Verify

### Requirements
- JDK 17 or higher.

### Step 1: Compile the Project
Open your terminal and navigate to the project directory:
```bash
cd src
javac *.java
```

### Step 2: Run the Server
Start the server specifying the port, maximum database key capacity, and snapshot interval (in seconds):
```bash
# Usage: java Server [port] [maxCapacity] [snapshotIntervalSec]
java Server 6380 10000 300
```

### Step 3: Connect and Interact
In another terminal, connect using `telnet` or `nc`:
```bash
telnet localhost 6380
```
Try running some commands:
```text
SET name gopal
GET name
EXPIRE name 30
SIZE
QUIT
```

---

## 🧪 Running Verification Tests

The project includes a robust stress-test client (`TestClient`) to verify concurrency safety under heavy loads, key expiry, LRU cache eviction thresholds, and write-ahead log recovery.

To run the full test suite:
1. **Start the server** in one terminal with a limited capacity of 10 keys:
   ```bash
   java Server 6380 10
   ```
2. **Run the test client** in another terminal:
   ```bash
   # Usage: java TestClient [host] [port] [numThreads] [opsPerThread]
   java TestClient localhost 6380 20 100
   ```

### What the Test Client Verifies:
- **Test 1 (Concurrency):** Runs 20 concurrent threads doing 100 writes/reads each. Proves that read-write locking prevents memory corruption.
- **Test 2 (Expiry):** Sets a key with a 2-second TTL, verifies it exists immediately, waits 3 seconds, and checks that it is successfully purged.
- **Test 3 (LRU Eviction):** Writes 15 keys to a server with a cap of 10 keys. Asserts that the oldest keys are evicted to keep the size capped at 10.
- **Test 4 (WAL Durability):** Sets a sequence of crash-resilient keys. Instructs you to kill the server (`Ctrl+C`) and restart it, then re-runs to prove that the keys survived the crash.
