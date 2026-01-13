package com.sumit.keydb.keydb.dto.request;

import java.util.Map;

public record BatchPutRequest(
        Map<String, String> entries
) {}