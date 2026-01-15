package com.sumit.keydb.keydb.controller;

import com.sumit.keydb.keydb.dto.request.BatchPutRequest;
import com.sumit.keydb.keydb.dto.request.PutRequest;
import com.sumit.keydb.keydb.exception.KeyDBException;
import com.sumit.keydb.keydb.exception.KeyNotFoundException;
import com.sumit.keydb.keydb.replication.ReplicationCoordinator;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping(value = "${controller.keydb}")
public class KeyDBController {

    private final ReplicationCoordinator replicationCoordinator;

    public KeyDBController(final ReplicationCoordinator replicationCoordinator) {
        this.replicationCoordinator = replicationCoordinator;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<String> put(@RequestBody PutRequest putRequest) throws KeyDBException {
        this.replicationCoordinator.put(putRequest.key(), putRequest.value(), this.replicationCoordinator.getVersionClock());
        return new ResponseEntity<>("OK", HttpStatus.CREATED);
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> get(@PathVariable String key) {
        String value = this.replicationCoordinator.get(key);
        if (!StringUtils.hasLength(value)) {
            throw new KeyNotFoundException(key);
        }
        return new ResponseEntity<>(value, HttpStatus.OK);
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<String> delete(@PathVariable String key) {
        this.replicationCoordinator.delete(key);
        return new ResponseEntity<>("OK", HttpStatus.OK);
    }

    @PostMapping("/batch")
    public ResponseEntity<String> batch(@RequestBody BatchPutRequest request) {
        this.replicationCoordinator.batchPut(request.entries());
        return new ResponseEntity<>("OK", HttpStatus.CREATED);
    }

    @GetMapping("/range")
    public ResponseEntity<Map<String, String>> range(@RequestParam String start, @RequestParam String end) throws IOException {
        Map<String, String> map = this.replicationCoordinator.range(start, end);
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @PostMapping("/compact")
    public ResponseEntity<String> compaction(@RequestParam(required = false) String replicaId) {
        try {
            this.replicationCoordinator.compaction();
            return new ResponseEntity<>(HttpStatus.OK.name(), HttpStatus.OK);
        } catch (Exception e) {
            throw new KeyDBException("Failed to compact key");
        }
    }
}
