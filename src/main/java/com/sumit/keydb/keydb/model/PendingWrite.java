package com.sumit.keydb.keydb.model;

public record PendingWrite(String key, String value, long timestamp) {
}
