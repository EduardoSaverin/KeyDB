package com.sumit.keydb.keydb.common;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "storage")
@Getter
@Setter
public class StorageConfig {
    private String dataDir;
    private long maxFileSize;
}
