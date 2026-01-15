package com.sumit.keydb.keydb.engine;

import com.sumit.keydb.keydb.common.StorageConfig;
import com.sumit.keydb.keydb.model.KeyDirEntry;
import com.sumit.keydb.keydb.model.LogEntry;
import com.sumit.keydb.keydb.model.ValueWithMeta;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

@Component
@Log4j2
public class KeyDBEngine {
    private final Path dataDir;
    private final long MAX_SEGMENT_SIZE;
    private final Map<String, KeyDirEntry> keyDir = new ConcurrentHashMap<>();
    private final Map<Integer, RandomAccessFile> fileHandles = new ConcurrentHashMap<>();
    private final Set<Integer> sealedFiles = ConcurrentHashMap.newKeySet();
    private final Set<Integer> missingHintFiles = ConcurrentHashMap.newKeySet();
    private final ExecutorService hintBuilder = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    });
    private final ReentrantLock WRITE_LOCK = new ReentrantLock();
    private final AtomicLong versionClock = new AtomicLong();
    private RandomAccessFile activeFile;
    private int activeFileId;
    private long activeOffset;

    private int pendingWrites = 0;
    private long lastSyncTime = System.nanoTime();
    private static final int SYNC_EVERY_N = 2_000;
    private static final long SYNC_EVERY_NS = 20_000_000;

    public KeyDBEngine(StorageConfig storageConfig) {
        log.info("Data Directory : {}", storageConfig.getDataDir());
        this.MAX_SEGMENT_SIZE = storageConfig.getMaxFileSize();
        this.dataDir = Paths.get(storageConfig.getDataDir());
    }

    @PostConstruct
    void init() throws IOException {
        Files.createDirectories(dataDir);
        initialize();
    }

    public void initialize() throws IOException {
        loadFromDisk();
        activeFileId = nextFileId();
        activeFile = openFile(activeFileId);
        activeOffset = activeFile.length();
        hintBuilder.submit(this::buildMissingHints);
    }

    private void loadFromDisk() throws IOException {
        List<Path> logFiles = Files.list(this.dataDir)
                .filter(item -> item.getFileName().toString().endsWith(".log"))
                .sorted()
                .toList();

        // Building KeyDir
        for (Path logFile : logFiles) {
            int fileId = extractFileId(logFile);
            RandomAccessFile raf = openFile(fileId);
            Path hintPath = hintFilePath(fileId);
            if (Files.exists(hintPath)) {
                loadFromHintFile(hintPath, fileId);
            } else {
                rebuildKeyDirFromLog(raf, fileId);
                missingHintFiles.add(fileId);
            }
        }
    }

    /* ===================== Hint Files ===================== */
    private void writeHintFile(int fileId) throws IOException {
        Path hintPath = hintFilePath(fileId);
        Path tmpHintPath = tmpHintFilePath(fileId);
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(tmpHintPath)))) {
            for (var e : this.keyDir.entrySet()) {
                KeyDirEntry entry = e.getValue();
                if (entry.fileId() != fileId) {
                    continue;
                }
                byte[] keyBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeLong(entry.valueOffset());
                out.writeInt(entry.valueLength());
                out.writeLong(entry.timestamp());
            }
        }
        Files.move(
                tmpHintPath,
                hintPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    private void writeHintFileFromEntries(int fileId, Map<String, KeyDirEntry> entries) throws IOException {
        Path hint = hintFilePath(fileId);
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(hint)))) {
            for (var e : entries.entrySet()) {
                byte[] keyBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
                KeyDirEntry dirEntry = e.getValue();
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeLong(dirEntry.valueOffset());
                out.writeInt(dirEntry.valueLength());
                out.writeLong(dirEntry.timestamp());
            }
        }
    }

    private void buildMissingHints() {
        for (Iterator<Integer> it = missingHintFiles.iterator(); it.hasNext(); ) {
            int fileId = it.next();
            try {
                writeHintFile(fileId);
                it.remove();
            } catch (IOException e) {
                log.warn("Failed to build hint file for {}", fileId, e);
            }
        }
    }

    private void loadFromHintFile(Path hintFilePath, int fileId) throws IOException {
        try (DataInputStream in =
                     new DataInputStream(
                             new BufferedInputStream(Files.newInputStream(hintFilePath)))) {

            while (true) {
                try {
                    int keySize = in.readInt();

                    byte[] keyBytes = new byte[keySize];
                    in.readFully(keyBytes);

                    long valueOffset = in.readLong();
                    int valueSize = in.readInt();
                    long timestamp = in.readLong();

                    String key = new String(keyBytes, StandardCharsets.UTF_8);
                    keyDir.put(
                            key,
                            new KeyDirEntry(fileId, valueOffset, valueSize, timestamp)
                    );
                } catch (EOFException eof) {
                    break;
                }
            }
        }
    }

    /* ===================== Log Scan Fallback ===================== */
    private void rebuildKeyDirFromLog(RandomAccessFile raf, int fileId) throws IOException {
        long offset = 0;
        long fileLength = raf.length();
        while (offset < raf.length()) {
            if (offset + LogEntry.HEADER_SIZE > fileLength) {
                break;
            }
            raf.seek(offset);
            // Read Header
            byte[] header = new byte[LogEntry.HEADER_SIZE];
            raf.readFully(header);

            ByteBuffer byteBuffer = ByteBuffer.wrap(header);
            int crc = byteBuffer.getInt(); // This is CRC as in Notion Diagram
            long timestamp = byteBuffer.getLong(); // timestamp
            int keySize = byteBuffer.getInt();
            int valueSize = byteBuffer.getInt();

            if (keySize <= 0 || keySize > 1024 * 1024) {
                log.warn("Invalid keySize {} at offset {} in file {}", keySize, offset, fileId);
                break;
            }
            if (valueSize < -1 || valueSize > 1024 * 1024 * 10) {
                log.warn("Invalid valueSize {} at offset {} in file {}", valueSize, offset, fileId);
                break;
            }

            int valueLen = Math.max(valueSize, 0);
            int recordSize = LogEntry.HEADER_SIZE + keySize + valueLen;

            // Ensure full record exists
            if (offset + recordSize > fileLength) {
                log.warn("Truncated record at offset {} in file {}", offset, fileId);
                break;
            }

            // Read full record
            byte[] record = new byte[recordSize];
            raf.seek(offset);
            raf.readFully(record);

            // CRC Validation
            CRC32 crc32 = new CRC32();
            crc32.update(record, 4, record.length - 4); // exclude CRC field
            int computedCrc = (int) crc32.getValue();

            if (computedCrc != crc) {
                log.warn(
                        "CRC mismatch at offset {} in file {} (expected {}, got {})",
                        offset, fileId, crc, computedCrc
                );
                break;
            }

            // Key
            int pos = LogEntry.HEADER_SIZE;
            byte[] keyBytes = Arrays.copyOfRange(record, pos, pos + keySize);

            String key = new String(keyBytes, StandardCharsets.UTF_8);
            if (valueSize == -1) {
                // Tombstone
                KeyDirEntry existing = keyDir.get(key);
                if (existing == null || existing.timestamp() < timestamp) {
                    keyDir.put(key, new KeyDirEntry(fileId, -1, -1, timestamp));
                }
            } else {
                // Value
                long valueOffset = offset + LogEntry.HEADER_SIZE + keySize;
                KeyDirEntry existing = keyDir.get(key);
                if (existing == null || existing.timestamp() < timestamp) {
                    keyDir.put(
                            key,
                            new KeyDirEntry(fileId, valueOffset, valueSize, timestamp)
                    );
                }
            }
            offset += recordSize;
        }
    }

    /* ===================== Write Operations ===================== */
    public void put(String key, String value, long timestamp) throws IOException {
        if (value == null) {
            this.delete(key);
            return;
        }
        WRITE_LOCK.lock();
        try {
            rotateIfNeeded();
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] data = LogEntry.serializeWithTimestamp(keyBytes, valueBytes, timestamp);

            long offset = activeOffset;
            FileChannel channel = activeFile.getChannel();
            channel.write(ByteBuffer.wrap(data), offset);
            activeOffset += data.length;
            pendingWrites++;

            long now = System.nanoTime();
            if (pendingWrites >= SYNC_EVERY_N || now - lastSyncTime > SYNC_EVERY_NS) {
                activeFile.getFD().sync();
                pendingWrites = 0;
                lastSyncTime = now;
            }

            long valueOffset = offset + LogEntry.HEADER_SIZE + keyBytes.length;
            keyDir.compute(key, (k, existing) -> {
                if (existing == null || existing.timestamp() < timestamp) {
                    return new KeyDirEntry(
                            activeFileId,
                            valueOffset,
                            valueBytes.length,
                            timestamp
                    );
                }
                return existing;
            });
        } finally {
            WRITE_LOCK.unlock();
        }
    }

    public void delete(String key) throws IOException {
        WRITE_LOCK.lock();
        try {
            rotateIfNeeded();
            byte[] data = LogEntry.serializeWithTimestamp(key.getBytes(StandardCharsets.UTF_8), null, getVersionClock());
            activeFile.seek(activeFile.getFilePointer());
            activeFile.write(data);
            activeFile.getFD().sync();
            this.keyDir.remove(key);
            activeOffset = 0;
        } finally {
            WRITE_LOCK.unlock();
        }
    }

    public void batchPut(Map<String, String> items) throws IOException {
        for (var e : items.entrySet()) {
            this.put(e.getKey(), e.getValue(), getVersionClock());
        }
    }

    /* ===================== Read Operations ===================== */

    public String get(String key) throws IOException {
        KeyDirEntry entry = this.keyDir.get(key);
        if (entry == null) {
            return null;
        }
        RandomAccessFile raf = openFile(entry.fileId());
        if (entry.valueOffset() + entry.valueLength() > raf.length()) {
            throw new IOException("Corrupted offset for key: " + key);
        }

        FileChannel channel = raf.getChannel();
        ByteBuffer buf = ByteBuffer.allocate(entry.valueLength());
        channel.read(buf, entry.valueOffset());
        return new String(buf.array(), StandardCharsets.UTF_8);
    }

    public ValueWithMeta getWithMeta(String key) throws IOException {
        KeyDirEntry entry = this.keyDir.get(key);
        if (entry == null) {
            return null;
        }
        RandomAccessFile raf = openFile(entry.fileId());
        if (entry.valueOffset() + entry.valueLength() > raf.length()) {
            throw new IOException("Corrupted offset for key: " + key);
        }
        FileChannel channel = raf.getChannel();
        ByteBuffer buf = ByteBuffer.allocate(entry.valueLength());
        channel.read(buf, entry.valueOffset());
        return new ValueWithMeta(new String(buf.array(), StandardCharsets.UTF_8), entry.timestamp());
    }

    public Map<String, String> range(String start, String end) throws IOException {
        Map<String, String> result = new TreeMap<>();
        for (String key : keyDir.keySet()) {
            if (key.compareTo(start) >= 0 && key.compareTo(end) <= 0) {
                result.put(key, get(key));
            }
        }
        return result;
    }

    public Map<String, ValueWithMeta> rangeWithMeta(String start, String end) throws IOException {
        Map<String, ValueWithMeta> result = new TreeMap<>();
        for (String key : keyDir.keySet()) {
            if (key.compareTo(start) >= 0 && key.compareTo(end) <= 0) {
                result.put(key, getWithMeta(key));
            }
        }
        return result;
    }

    /* ===================== Compaction ===================== */
    public void compaction() throws IOException {
        log.debug("Compaction started");

        // Snapshot
        Map<String, KeyDirEntry> snapshot;
        Set<Integer> compactableFiles;

        WRITE_LOCK.lock();
        try {
            snapshot = new HashMap<>(keyDir);
            compactableFiles = new HashSet<>(sealedFiles);
            compactableFiles.remove(activeFileId);
            log.debug("Snapshot created");
        } finally {
            WRITE_LOCK.unlock();
        }

        if (compactableFiles.isEmpty()) {
            return;
        }

        // Copy
        log.debug("Starting copy");
        int mergeFileId = nextFileId();
        long mergeOffset = 0;
        RandomAccessFile mergeFile = openFile(mergeFileId);
        Map<String, KeyDirEntry> mergedEntries = new HashMap<>();
        for (var e : snapshot.entrySet()) {
            String key = e.getKey();
            KeyDirEntry entry = e.getValue();
            if (!compactableFiles.contains(entry.fileId())) {
                continue;
            }
            if (mergedEntries.containsKey(key)) {
                if (mergedEntries.get(key).timestamp() > entry.timestamp()) {
                    continue;
                }
            }
            if (entry.valueLength() < 0) {
                // Tombstone —  we do not copy this
                continue;
            }
            RandomAccessFile src = openFile(entry.fileId());
            src.seek(entry.valueOffset());
            byte[] valueBytes = new byte[entry.valueLength()];
            src.readFully(valueBytes);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            System.out.println("Value:" + new String(valueBytes, StandardCharsets.UTF_8));
            // Record
            long recordOffset = entry.valueOffset() - (LogEntry.HEADER_SIZE + keyBytes.length);
            src.seek(recordOffset);
            byte[] record = new byte[LogEntry.HEADER_SIZE + keyBytes.length + entry.valueLength()];
            src.readFully(record);

            //byte[] logEntry = LogEntry.serializeWithTimestamp(keyBytes, valueBytes, entry.timestamp());
            mergeFile.seek(mergeOffset);
            mergeFile.write(record);

            long valueOffset = mergeOffset + LogEntry.HEADER_SIZE + keyBytes.length;
            KeyDirEntry newEntry = new KeyDirEntry(mergeFileId, valueOffset, valueBytes.length, entry.timestamp());
            mergedEntries.put(key, newEntry);
            mergeOffset += record.length;
        }
        log.debug("Compacted File Entry Size: {}, Merge File ID: {}", mergedEntries.size(), mergeFileId);
        mergeFile.getFD().sync();
        writeHintFileFromEntries(mergeFileId, mergedEntries);

        // Swapping
        log.debug("Swapping files");
        for (var e : mergedEntries.entrySet()) {
            KeyDirEntry current = keyDir.get(e.getKey());
            KeyDirEntry newEntry = e.getValue();
            if (current != null && compactableFiles.contains(current.fileId())) {
                fileHandles.put(newEntry.fileId(), mergeFile);
                // Issue was here putting blindly value
                // keyDir.put(e.getKey(), newEntry);
                if (current.timestamp() < newEntry.timestamp()) {
                    keyDir.put(e.getKey(), newEntry);
                }
                // closeAndDelete(current.fileId());
            }
        }
        log.debug("Compaction finished");
    }

    /* ===================== File Utilities ===================== */

    private void closeAndDelete(int fileId) throws IOException {
        RandomAccessFile raf = fileHandles.remove(fileId);
        if (raf != null) {
            raf.close();
        }
        Files.deleteIfExists(hintFilePath(fileId));
        Files.deleteIfExists(logFilePath(fileId));
    }

    private void rotateIfNeeded() throws IOException {
            if (activeFile.length() < MAX_SEGMENT_SIZE) {
                return;
            }
            // Swapping Atomically
            int newFileId = nextFileId();
            RandomAccessFile newFile = openFile(newFileId);
            RandomAccessFile oldFile = activeFile;
            activeFile = newFile;

            int oldFileId = activeFileId;
            activeFileId = newFileId;
            activeOffset = 0;

            oldFile.getFD().sync();
            writeHintFile(oldFileId);
            sealedFiles.add(oldFileId);
    }

    private RandomAccessFile openFile(int fileId) {
        return fileHandles.computeIfAbsent(fileId, id -> {
            try {
                Path path = logFilePath(id);
                return new RandomAccessFile(path.toFile(), "rw");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public void close() {
        this.hintBuilder.shutdownNow();
        for (RandomAccessFile raf : this.fileHandles.values()) {
            try {
                raf.close();
            } catch (IOException ignored) {}
        }
        this.fileHandles.clear();
    }

    // File Handles has all log files since we ran load from disk before this.
    private int nextFileId() {
        return fileHandles.keySet().stream()
                .max(Integer::compareTo)
                .orElse(0) + 1;
    }

    private int extractFileId(Path path) {
        String fileName = path.getFileName().toString();
        return Integer.parseInt(fileName.replace("data_", "").replace(".log", ""));
    }

    private Path logFilePath(int fileId) {
        return dataDir.resolve("data_" + fileId + ".log");
    }

    private Path hintFilePath(int fileId) {
        return dataDir.resolve("data_" + fileId + ".hint");
    }

    private Path tmpHintFilePath(int fileId) {
        return dataDir.resolve("data_" + fileId + ".hint.tmp");
    }

    public long getVersionClock() {
        return versionClock.incrementAndGet();
    }
}
