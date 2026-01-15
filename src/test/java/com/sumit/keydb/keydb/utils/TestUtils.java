package com.sumit.keydb.keydb.utils;

import com.sumit.keydb.keydb.common.StorageConfig;
import com.sumit.keydb.keydb.engine.KeyDBEngine;

import java.io.IOException;
import java.nio.file.Path;

public final class TestUtils {
    private TestUtils() {}

    public static KeyDBEngine createEngine(Path dataDir) throws IOException {
        StorageConfig config = new StorageConfig();
        config.setDataDir(dataDir.toString());
        config.setMaxFileSize(1024 * 1024);
        KeyDBEngine engine = new KeyDBEngine(config);
        engine.initialize();

        return engine;
    }
}
