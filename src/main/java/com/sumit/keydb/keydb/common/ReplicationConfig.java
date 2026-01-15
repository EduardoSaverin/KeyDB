package com.sumit.keydb.keydb.common;

import com.sumit.keydb.keydb.engine.KeyDBEngine;
import com.sumit.keydb.keydb.model.Replica;
import com.sumit.keydb.keydb.replication.HintedHandoffService;
import com.sumit.keydb.keydb.replication.ReplicationCoordinator;
import lombok.Getter;
import lombok.Setter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@Getter
@Setter
public class ReplicationConfig {

    @Bean
    public ReplicationCoordinator replicationCoordinator(
            KeyDBEngine e1,
            KeyDBEngine e2,
            KeyDBEngine e3,
            HintedHandoffService hintedHandoffService,
            ReplicationProperties replicationProperties
    ) {
        return new ReplicationCoordinator(
                List.of(
                        new Replica("r1", e1),
                        new Replica("r2", e2),
                        new Replica("r3", e3)
                ),
                replicationProperties,
                hintedHandoffService
        );
    }
}
