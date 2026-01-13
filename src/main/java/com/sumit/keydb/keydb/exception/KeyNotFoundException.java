package com.sumit.keydb.keydb.exception;

public class KeyNotFoundException extends KeyDBException {
    public KeyNotFoundException(String key) {
        super("Key not found: " + key);
    }
}