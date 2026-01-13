package com.sumit.keydb.keydb.dto.request;

public record PutRequest(
        String key,
        String value
) {}