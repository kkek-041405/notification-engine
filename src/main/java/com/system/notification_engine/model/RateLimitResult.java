package com.system.notification_engine.model;

/**
 * Encapsulates the decision returned by the Rate Limiter.
 *
 * @param allowed        true if request is permitted, false if rate limited.
 * @param remainingQuota how many more requests the client can make in the active window.
 */
public record RateLimitResult(boolean allowed, long remainingQuota) {
}
