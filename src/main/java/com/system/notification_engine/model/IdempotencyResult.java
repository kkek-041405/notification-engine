package com.system.notification_engine.model;


public record IdempotencyResult(Status status,String cachedResponse) {


    public enum Status {
        ACQUIRED,
        CONFLICT,
        ALREADY_COMPLETED
    }


    public static IdempotencyResult acquired() {
        return new IdempotencyResult(Status.ACQUIRED, null);
    }

    public static IdempotencyResult conflict() {
        return new IdempotencyResult(Status.CONFLICT, null);
    }

    public static IdempotencyResult alreadyCompleted(String cachedResponse) {
        return new IdempotencyResult(Status.ALREADY_COMPLETED, cachedResponse);
    }
}