package com.sumit.keydb.keydb.engine;

import com.sumit.keydb.keydb.common.StorageConfig;
import com.sumit.keydb.keydb.model.KeyDirEntry;
import com.sumit.keydb.keydb.model.LogEntry;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

@Component
@Log4j2
public class KeyDBEngine {
    private final Path dataDir;
    private final long MAX_SEGMENT_SIZE;
    private final Map<String, KeyDirEntry> keyDir = new ConcurrentHashMap<>();
    private final Map<Integer, RandomAccessFile> fileHandles = new ConcurrentHashMap<>();
    private final Set<Integer> missingHintFiles = ConcurrentHashMap.newKeySet();
    private final ExecutorService hintBuilder = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    });
    private final ReentrantLock WRITE_LOCK = new ReentrantLock();

    private RandomAccessFile activeFile;
    private int activeFileId;

    private int pendingWrites = 0;
    private long lastSyncTime = System.nanoTime();
    private static final int SYNC_EVERY_N = 500;
    private static final long SYNC_EVERY_NS = 5_000_000;

    public KeyDBEngine(StorageConfig storageConfig) {
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
        Path tmpHintPath = hintFilePath(fileId);
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
                out.writeLong(System.currentTimeMillis());
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
                out.writeLong(System.currentTimeMillis());
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
        while (offset < raf.length()) {
            raf.seek(offset);
            byte[] header = new byte[LogEntry.HEADER_SIZE];
            raf.readFully(header);

            ByteBuffer byteBuffer = ByteBuffer.wrap(header);
            int crc = byteBuffer.getInt(); // This is CRC as in Notion Diagram
            long timestamp = byteBuffer.getLong(); // timestamp
            int keySize = byteBuffer.getInt();
            int valueSize = byteBuffer.getInt();
            // Key
            byte[] keyBytes = new byte[keySize];
            raf.readFully(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            if (valueSize == -1) {
                // Tombstone
                this.keyDir.remove(key);
            } else {
                // Value
                long valueOffset = offset + LogEntry.HEADER_SIZE + keySize;
                boolean addEntry = true;
                if (keyDir.containsKey(key)) {
                    KeyDirEntry entry = keyDir.get(key);
                    if (entry.timestamp() > timestamp) {
                        // Key Dir Already has new value
                        addEntry = false;
                    }
                }
                if (addEntry) {
                    this.keyDir.put(key, new KeyDirEntry(fileId, valueOffset, valueSize, timestamp));
                }
            }
            offset += LogEntry.HEADER_SIZE + keySize + Math.max(valueSize, 0);
        }
    }

    /* ===================== Write Operations ===================== */
    public void put(String key, String value) throws IOException {
        WRITE_LOCK.lock();
        try {
            rotateIfNeeded();
            byte[] data = LogEntry.serialize(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
            long offset = activeFile.length();
            activeFile.seek(offset);
            activeFile.write(data);
            pendingWrites++;

            long now = System.nanoTime();
            if (pendingWrites >= SYNC_EVERY_N || now - lastSyncTime > SYNC_EVERY_NS) {
                activeFile.getFD().sync();
                pendingWrites = 0;
                lastSyncTime = now;
            }

            long valueOffset = offset + LogEntry.HEADER_SIZE + key.getBytes(StandardCharsets.UTF_8).length;
            if ("v0".equals(value)) {
                System.out.println("Length: " + value.length());
            }
            this.keyDir.put(key, new KeyDirEntry(activeFileId, valueOffset, value.length(), System.currentTimeMillis()));
        } finally {
            WRITE_LOCK.unlock();
        }
    }

    public void delete(String key) throws IOException {
        WRITE_LOCK.lock();
        try {
            rotateIfNeeded();
            byte[] data = LogEntry.serialize(key.getBytes(StandardCharsets.UTF_8), null);
            activeFile.seek(activeFile.length());
            activeFile.write(data);
            activeFile.getFD().sync();
            this.keyDir.remove(key);
        } finally {
            WRITE_LOCK.unlock();
        }
    }

    public void batchPut(Map<String, String> items) throws IOException {
        WRITE_LOCK.lock();
        try {
            for (var e : items.entrySet()) {
                this.put(e.getKey(), e.getValue());
            }
        } finally {
            WRITE_LOCK.unlock();
        }
    }

    /* ===================== Read Operations ===================== */

    public String get(String key) throws IOException {
        KeyDirEntry entry = this.keyDir.get(key);
        if (entry == null) {
            return null;
        }
        System.out.println("Reading File ID: " + entry.fileId());
        RandomAccessFile raf = openFile(entry.fileId());
        raf.seek(entry.valueOffset());
        byte[] value = new byte[entry.valueLength()];
        raf.readFully(value);
        return new String(value);
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

    /* ===================== Compaction ===================== */
    public void compaction() throws IOException {
        log.debug("Compaction started");

        // Snapshot
        Map<String, KeyDirEntry> snapshot;
        Set<Integer> compactableFiles;

        WRITE_LOCK.lock();
        try {
            snapshot = new HashMap<>(keyDir);
            compactableFiles = new HashSet<>(fileHandles.keySet());
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
        RandomAccessFile mergeFile = openFile(mergeFileId);
        Map<String, KeyDirEntry> mergedEntries = new HashMap<>();
        for (var e : snapshot.entrySet()) {
            String key = e.getKey();
            KeyDirEntry entry = e.getValue();
            if (!compactableFiles.contains(entry.fileId())) {
                continue;
            }
            RandomAccessFile src = openFile(entry.fileId());
            src.seek(entry.valueOffset());

            byte[] valueBytes = new byte[entry.valueLength()];
            src.readFully(valueBytes);

            String value = new String(valueBytes, StandardCharsets.UTF_8);
            if ("k0".equals(key)) {
                System.out.println("Key: " + key + " Value: " + value);
            }

            byte[] logEntry = LogEntry.serialize(key.getBytes(StandardCharsets.UTF_8), valueBytes);
            long offset = mergeFile.length();
            mergeFile.seek(offset);
            mergeFile.write(logEntry);

            long valueOffset = offset + LogEntry.HEADER_SIZE + key.getBytes(StandardCharsets.UTF_8).length;
            KeyDirEntry newEntry = new KeyDirEntry(mergeFileId, valueOffset, value.length(), entry.timestamp());
            if (mergedEntries.containsKey(key)) {
                KeyDirEntry existingMergedEntry = mergedEntries.get(value);
                if (existingMergedEntry.timestamp() > entry.timestamp()) {
                    newEntry = new KeyDirEntry(mergeFileId, valueOffset, existingMergedEntry.valueLength(), existingMergedEntry.timestamp());
                }
            }
            mergedEntries.put(key, newEntry);
        }
        log.debug("Compacted File Entry Size: {}, Merge File ID: {}", mergedEntries.size(), mergeFileId);
        mergeFile.getFD().sync();
        writeHintFileFromEntries(mergeFileId, mergedEntries);

        // Swapping
        log.debug("Swapping files");
        WRITE_LOCK.lock();
        try {
            for (var e : mergedEntries.entrySet()) {
                KeyDirEntry current = keyDir.get(e.getKey());
                KeyDirEntry newEntry = e.getValue();
                if (current != null && compactableFiles.contains(current.fileId())) {
                    System.out.println("New File ID: " + newEntry.fileId() + " Current File ID: " + current.fileId());
                    fileHandles.put(newEntry.fileId(), mergeFile);
                    keyDir.put(e.getKey(), newEntry);
                    // closeAndDelete(current.fileId());
                }
            }
        } finally {
            WRITE_LOCK.unlock();
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
        WRITE_LOCK.lock();
        try {
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

            oldFile.getFD().sync();
            writeHintFile(oldFileId);
        } finally {
            WRITE_LOCK.unlock();
        }
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
}
