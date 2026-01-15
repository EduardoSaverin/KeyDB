# KeyDB

KeyDB is a **network-accessible, persistent Key/Value storage engine** inspired by the Bitcask design. It is implemented in **Java (Spring Boot)** using **only standard JDK libraries** for storage, concurrency, and durability.

This project was built as part of a **storage engine / database systems take-home assignment** and focuses on correctness, crash safety, predictability under load, and clear trade-offs.

---

## ✨ High-Level Features

* Persistent append-only log storage
* In-memory KeyDir index (key → fileId, offset, size, timestamp)
* Segment-based log files with automatic rotation
* Hint files for fast startup and recovery
* Background hint-file rebuilding
* Safe concurrent reads and writes
* Crash-safe recovery without data loss
* Manual compaction to reclaim disk space
* REST API for interaction

---

## 🧱 Architecture Overview

```
                +--------------------+
                |  REST API (HTTP)   |
                +----------+---------+
                           |
                           v
                +--------------------+
                |    KeyDBEngine     |
                |--------------------|
                |  KeyDir (in-mem)   |
                |  Active Segment    |
                |  Old Segments      |
                +----------+---------+
                           |
                           v
                +--------------------+
                |  Append-only Logs  |
                |  + Hint Files      |
                +--------------------+
```

---

## 🗂 Storage Layout

Each key/value write is appended to a log segment:

```
| CRC | Timestamp | KeySize | ValueSize | Key | Value |
```

* **Append-only** (no in-place updates)
* Deletes are stored as **tombstones**
* Data files: `data_<id>.log`
* Hint files: `data_<id>.hint`

---

## 🧠 KeyDir (In-Memory Index)

The KeyDir holds metadata for the **latest version of each key**:

```java
key → (fileId, valueOffset, valueLength, timestamp)
```

* Allows **O(1)** reads
* Fits in memory even when data exceeds RAM
* Rebuilt from hint files or logs during startup

---

## ⚡ Startup & Recovery

On startup:

1. Scan all `.log` files
2. If `.hint` exists → load KeyDir from hint file
3. Else → rebuild KeyDir by scanning the log
4. Missing hint files are queued for **background rebuild**

This guarantees:

* Fast startup
* Correct recovery after crashes
* No data loss

---

## 🔄 Log Rotation

* Active log rotates when it exceeds `MAX_SEGMENT_SIZE`
* Rotation is **atomic with respect to writers**
* Old segments remain readable
* Hint file is generated for the rotated segment

Design choice:

* Old files are **not closed immediately** to avoid breaking concurrent readers
* They are closed only during compaction

---

## 🧹 Compaction

Compaction:

* Creates a **snapshot** of the KeyDir
* Copies only the **latest live keys** from old segments
* Writes them into a new merged segment
* Atomically swaps KeyDir entries
* Deletes obsolete log + hint files

This reclaims disk space while keeping reads available.

---

## 🔒 Concurrency Model

* **Writes** protected by a single write lock
* **Reads** are lock-free
* One `RandomAccessFile` per segment (shared, never duplicated)
* `RandomAccessFile` is backed with `FileChannel` to make it thread-safe
* File handles are reused safely via `computeIfAbsent`

Guarantees:

* No `StreamClosed` errors
* No torn state during rotation
* Predictable behavior under load

---

## 🔄 Replication Model
All client requests are handled by a central `ReplicationCoordinator`.

Each replica consists of:

* A unique replica ID
* An independent `KeyDBEngine`
* A dedicated data directory
* Its own in-memory key directory (`keyDir`)

Storage engines are replication-agnostic. All quorum logic, retries, and repairs are managed by the coordinator.

---
## ☢️ Quorum Configuration (N, W, R)

Replication behavior is controlled using three parameters:
* **N** – Total number of replicas
* **W** – Write quorum (minimum replicas required to acknowledge a write)
* **R** – Read quorum (minimum replicas required to answer a read)

The system follows the quorum rule:
```java
R + W > N
```
This ensures that at least one replica in every read quorum has observed the most recent successful write.

---

## 📝 Write Path (PUT / DELETE)
1. Each write is assigned a **monotonically increasing version** using a global version clock.
2. The write is dispatched **concurrently** to all replicas.
3. The coordinator waits for **W acknowledgements**.
4. Once W replicas respond successfully, the write is considered committed.
5. Failed replicas are handled using hinted handoff.

Conflict resolution uses **last-write-wins** based on version timestamps.

Writes are non-blocking with respect to slow or unavailable replicas.

---
## 👓 Read Path (GET)
1. Read requests are sent concurrently to all replicas.
2. The coordinator waits for **R responses**.
3. The value with the **highest version timestamp** is selected.
4. The value is immediately returned to the client.
5. **Read repair** is triggered asynchronously for stale replicas.

Reads prioritize low latency and never wait for repairs to complete.

---
## 🧑🏼‍🔧 Read Repair
When replicas return different versions of the same key:

* The coordinator identifies the newest version.
* Stale replicas are asynchronously updated.
* The client response is not delayed.

This allows replicas to converge naturally over time without coordination overhead.

---
## 🖐 Hinted Handoff

If a replica is unavailable during a write:
* The write is recorded as a **hint**.
* Hints are stored persistently.
* Once the replica becomes available, hints are replayed automatically.

This prevents data loss during temporary failures or restarts.

---
## 🔏 Consistency Guarantees

KeyDB provides **eventual consistency** with the following guarantees:

* Writes succeed as long as **W replicas** are reachable
* Reads return the latest value once **R replicas** respond
* Temporary inconsistencies are resolved automatically
* No committed data is lost
* Replica crashes are safely recovered via log replay

---

## 🚀 Performance Characteristics

Measured on local SSD:

| Configuration            | Throughput   |
|--------------------------|--------------|
| fsync per write          | ~8k ops/sec  |
| deferred fsync per write | ~10k ops/sec |

Trade-offs are explicitly documented and configurable.

---

## 🌐 REST API

| Method | Endpoint                | Description   |
| ------ | ----------------------- | ------------- |
| POST   | `/keydb`                   | Put key/value |
| POST   | `/keydb/batch`             | Batch put     |
| GET    | `/keydb/{key}`             | Get value     |
| DELETE | `/keydb/{key}`             | Delete key    |
| GET    | `/keydb/range?start=&end=` | Range query   |

---

## 🧪 Testing

The project includes:

* Unit tests for all engine operations
* Concurrency stress tests (multi-threaded reads/writes)
* Compaction correctness tests
* Throughput benchmarking

Special care is taken to:

* Properly shut down background threads
* Avoid file descriptor leaks
* Ensure deterministic cleanup

---

## 🧠 Design Trade-offs

| Decision           | Rationale                                 |
| ------------------ | ----------------------------------------- |
| Append-only logs   | Simple, fast writes, crash-safe           |
| In-memory index    | O(1) reads                                |
| Hint files         | Fast startup                              |
| No per-write fsync | High throughput (configurable durability) |
| Manual compaction  | Predictable behavior                      |

---

## 📌 Known Limitations

* Compaction is manual (can be automated)
* Durability level is configurable but not pluggable yet
* No TTL support (easy to add)

---

## 🔮 Future Improvements

* Automatic background compaction
* Async writer thread (this will improve write speed 10x)
* Reference-counted file handles
* Bloom filters for faster negative lookups
* Metrics (latency, throughput)

---

## 🧑‍💻 Running Locally

```bash
./mvnw spring-boot:run
```

Default server:

```
http://localhost:9091
```

---

## 📜 License

This project is for **educational and evaluation purposes**.

---

## 🙌 Acknowledgements

* Bitcask (Riak)
* Designing Data-Intensive Applications – Martin Kleppmann
* LSM-tree & WAL-based storage engines

---

If you are reviewing this project as part of an interview process: **thank you for your time** — this implementation intentionally prioritizes correctness, clarity, and explicit trade-offs over premature optimization.
