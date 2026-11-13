package com.system.notification_engine.idempotency;

import com.system.notification_engine.model.IdempotencyResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);

    private final StringRedisTemplate redisTemplate;

    private static final String PROCESSING = "PROCESSING";
    private static final String COMPLETED_PREFIX = "COMPLETED:";
    private static final String KEY_PREFIX = "IDEMPOTENCY:";

    private static final Duration LOCK_TTL = Duration.ofSeconds(30);
    private static final Duration COMPLETED_TTL = Duration.ofHours(24);

    public IdempotencyService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public IdempotencyResult tryAcquire(String idempotencyKey) {
        String redisKey = KEY_PREFIX + idempotencyKey;

        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(redisKey, PROCESSING, LOCK_TTL);

        if (Boolean.TRUE.equals(acquired)) {
            return IdempotencyResult.acquired();
        }

        String existingValue = redisTemplate.opsForValue().get(redisKey);

        if (existingValue == null) {
            return IdempotencyResult.conflict();
        }

        if (existingValue.equals(PROCESSING)) {
            return IdempotencyResult.conflict();
        } else if (existingValue.startsWith(COMPLETED_PREFIX)) {
            String cachedResponse = existingValue.substring(COMPLETED_PREFIX.length());
            return IdempotencyResult.alreadyCompleted(cachedResponse);
        } else {
            log.warn("Unexpected value in Redis for key {}: {}", redisKey, existingValue);
            return IdempotencyResult.conflict();
        }
    }

    public void markCompleted(String idempotencyKey, String response) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        String completedValue = COMPLETED_PREFIX + response;
        redisTemplate.opsForValue().set(redisKey, completedValue, COMPLETED_TTL);
        log.debug("Marked idempotency key {} as completed", idempotencyKey);
    }
}
