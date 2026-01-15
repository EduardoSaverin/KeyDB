package com.sumit.keydb.keydb.common;

import com.sumit.keydb.keydb.engine.KeyDBEngine;
import com.sumit.keydb.keydb.model.Replica;
import com.sumit.keydb.keydb.replication.HintedHandoffService;
import com.sumit.keydb.keydb.replication.ReplicationCoordinator;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "replication")
@Getter
@Setter
public class ReplicationProperties {
    /**
     * Replication factor
     */
    private int N = 3;

    /**
     * Write quorum
     */
    private int W = 2;

    /**
     * Read quorum
     */
    private int R = 1;

    private void validate() {
        if (N <= 0) {
            throw new IllegalStateException("replication.N must be > 0");
        }
        if (W <= 0 || W > N) {
            throw new IllegalStateException("replication.W must be in range [1, N]");
        }
        if (R <= 0 || R > N) {
            throw new IllegalStateException("replication.R must be in range [1, N]");
        }
    }
}
