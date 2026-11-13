package com.system.notification_engine.ratelimiter;

import com.system.notification_engine.model.RateLimitResult;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);

    private final StringRedisTemplate redisTemplate;

    @SuppressWarnings("rawtypes")
    private DefaultRedisScript<List> rateLimitScript;

    public RateLimiterService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void init() {
        rateLimitScript = new DefaultRedisScript<>();
        rateLimitScript.setResultType(List.class);
        rateLimitScript.setScriptSource(
                new ResourceScriptSource(new ClassPathResource("sliding_window.lua"))
        );
        log.info("RateLimiterService initialized with sliding_window.lua script.");
    }

    public RateLimitResult isAllowed(String rateLimitKey, long windowSizeMs, long maxRequests) {
        long now = System.currentTimeMillis();
        List<String> keys = Collections.singletonList(rateLimitKey);

        try {
            @SuppressWarnings("unchecked")
            List<Object> results = redisTemplate.execute(
                    rateLimitScript,
                    keys,
                    String.valueOf(now),
                    String.valueOf(windowSizeMs),
                    String.valueOf(maxRequests)
            );

            if (results == null || results.size() < 2) {
                log.warn("Unexpected result from rate limit script for key: {}", rateLimitKey);
                return new RateLimitResult(true, maxRequests - 1);
            }

            long status = Long.parseLong(results.get(0).toString());
            long remaining = Long.parseLong(results.get(1).toString());

            boolean isAllowed = (status == 1L);
            return new RateLimitResult(isAllowed, remaining);

        } catch (Exception e) {
            log.error("Error executing rate limit script: {}", e.getMessage(), e);
            return new RateLimitResult(true, 0);
        }
    }
}
