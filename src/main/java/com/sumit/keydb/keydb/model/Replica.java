package com.sumit.keydb.keydb.model;

import com.sumit.keydb.keydb.engine.KeyDBEngine;

public record Replica(String id, KeyDBEngine engine) {
}
