package com.sumit.keydb.keydb.exception;

public class KeyDBException extends RuntimeException {
    public KeyDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public KeyDBException(String message) {
        super(message);
    }
}