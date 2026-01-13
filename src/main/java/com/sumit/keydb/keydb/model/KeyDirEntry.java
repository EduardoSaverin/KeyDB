package com.sumit.keydb.keydb.model;

public record KeyDirEntry(int fileId, long valueOffset, int valueLength, long timestamp) { }
