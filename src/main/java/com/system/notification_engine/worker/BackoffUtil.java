package com.system.notification_engine.worker;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Implements Exponential Backoff with Full Jitter.
 * 
 * Reference: AWS Architecture Blog - "Exponential Backoff And Jitter"
 */
public class BackoffUtil {

    private static final long BASE_DELAY_MS = 1000;  // 1 second base
    private static final long MAX_DELAY_MS = 30000;  // 30 seconds cap

    /**
     * Calculates the jittered backoff delay in milliseconds.
     *
     * @param retryCount Current retry attempt (1, 2, 3...)
     * @return Milliseconds to wait before next retry attempt
     */
    public static long calculateJitteredBackoff(int retryCount) {
        // 1. Calculate exponential delay: Base * 2^retryCount
        long exponentialDelay = BASE_DELAY_MS * (1L << Math.min(retryCount, 10));

        // 2. Cap at maximum delay
        long cappedDelay = Math.min(exponentialDelay, MAX_DELAY_MS);

        // 3. Apply Full Jitter: random value between 0 and cappedDelay
        return ThreadLocalRandom.current().nextLong(0, cappedDelay + 1);
    }
}
