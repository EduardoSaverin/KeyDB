package com.sumit.keydb.keydb.controller;

import com.sumit.keydb.keydb.dto.request.BatchPutRequest;
import com.sumit.keydb.keydb.dto.request.PutRequest;
import com.sumit.keydb.keydb.engine.KeyDBEngine;
import com.sumit.keydb.keydb.exception.KeyDBException;
import com.sumit.keydb.keydb.exception.KeyNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping(value = "${controller.keydb}")
public class KeyDBController {

    private final KeyDBEngine keyDBEngine;

    public KeyDBController(KeyDBEngine keyDBEngine) {
        this.keyDBEngine = keyDBEngine;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ResponseEntity<String> put(@RequestBody PutRequest putRequest) throws KeyDBException {
        try {
            this.keyDBEngine.put(putRequest.key(), putRequest.value());
            return new ResponseEntity<>("OK", HttpStatus.CREATED);
        } catch (IOException e) {
            throw new KeyDBException("Failed to write key");
        }
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> get(@PathVariable String key) {
        try {
            String value = this.keyDBEngine.get(key);
            if (!StringUtils.hasLength(value)) {
                throw new KeyNotFoundException(key);
            }
            return new ResponseEntity<>(value, HttpStatus.OK);
        } catch (IOException e) {
            throw new KeyDBException("Failed to read key");
        }
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<String> delete(@PathVariable String key) {
        try {
            this.keyDBEngine.delete(key);
            return new ResponseEntity<>("OK", HttpStatus.OK);
        } catch (IOException e) {
            throw new KeyDBException("Failed to delete key");
        }
    }

    @PostMapping("/batch")
    public ResponseEntity<String> batch(@RequestBody BatchPutRequest request) {
        try {
            this.keyDBEngine.batchPut(request.entries());
            return new ResponseEntity<>("OK", HttpStatus.CREATED);
        } catch (IOException e) {
            throw new KeyDBException("Failed to write key");
        }
    }

    @GetMapping("/range")
    public ResponseEntity<Map<String, String>> range(@RequestParam String start, @RequestParam String end) throws IOException {
        Map<String, String> map = this.keyDBEngine.range(start, end);
        return new ResponseEntity<>(map, HttpStatus.OK);
    }

    @PostMapping("/compact")
    public ResponseEntity<String> compaction() {
        try {
            this.keyDBEngine.compaction();
            return new ResponseEntity<>(HttpStatus.OK.name(), HttpStatus.OK);
        } catch (Exception e) {
            throw new KeyDBException("Failed to compact key");
        }
    }
}
