# Distributed Notification Engine: Complete Architecture & Source Reference
*A Comprehensive Distributed Systems & High-Concurrency Guide for NotebookLM and Technical Mastery*

---

## Table of Contents
1. **Executive Architecture & System Blueprint**
2. **The 5 Core Distributed Pillars**
3. **Core Keyword & Annotation Glossary (Java, Spring, Redis)**
4. **Complete Source Code Repository**
   - 4.1 Redis Lua Rate Limiter (`sliding_window.lua`)
   - 4.2 Rate Limiter Service (`RateLimiterService.java` & `RateLimitResult.java`)
   - 4.3 Idempotency Service (`IdempotencyService.java` & `IdempotencyResult.java`)
   - 4.4 Domain Models (`ChannelType.java`, `NotificationRequest.java`, `NotificationResponse.java`)
   - 4.5 Queue Producer (`NotificationQueueProducer.java`)
   - 4.6 Channel Dispatcher (`NotificationDispatcher.java`)
   - 4.7 Exponential Backoff with Full Jitter (`BackoffUtil.java`)
   - 4.8 Stream Consumer & DLQ Engine (`NotificationStreamConsumer.java`)
   - 4.9 Ingestion Controller (`NotificationController.java`)
   - 4.10 Main Application (`NotificationEngineApplication.java`)
   - 4.11 Configuration (`application.properties` & `build.gradle`)
   - 4.12 Load Test Benchmark (`load_test.js`)
5. **Step-by-Step Request Lifecycle & Trace**
6. **Concurrency Hazards & Failure Modes Resolved**
7. **Empirical Benchmarks & Capacity Math**
8. **Interview Drill-Down Questions & Model Answers**

---

# 1. Executive Architecture & System Blueprint

Modern web applications fail during traffic spikes because ingestion (receiving requests) is tightly coupled to delivery (calling external third-party APIs like SendGrid, Twilio, or Stripe).

```
                      INCOMING HTTP TRAFFIC (3,000+ RPS)
                                     │
                                     ▼
                   ┌───────────────────────────────────┐
                   │   POST /api/v1/notify (Ingestion) │
                   └─────────────────┬─────────────────┘
                                     │
        ┌────────────────────────────┴────────────────────────────┐
        ▼                                                         ▼
┌─────────────────────────────────┐             ┌─────────────────────────────────┐
│ 1. Atomic Rate Limit Check      │             │ 2. Two-Phase Idempotency Lock   │
│    Algorithm: Sliding Window    │             │    Command: SETNX with 30s TTL  │
│    Engine: Redis Lua (EVALSHA)  │             │    Cache: COMPLETED (24h TTL)   │
└────────────────┬────────────────┘             └────────────────┬────────────────┘
                 └───────────────────────────┬───────────────────┘
                                             │ Valid & Non-Duplicate
                                             ▼
                             ┌───────────────────────────────┐
                             │ 3. Persistent Queue Ingestion │
                             │    Engine: Redis Streams      │
                             │    Command: XADD              │
                             └───────────────┬───────────────┘
                                             │ Returns 202 Accepted (< 5ms)
                                             ▼
                                     [ Client Receives ]
                                     { trackingId, QUEUED }
                                             │
                       ══════════════════════╪══════════════════════
                         ASYNC BOUNDARY      │ (Decoupled Delivery)
                       ══════════════════════╪══════════════════════
                                             ▼
                             ┌───────────────────────────────┐
                             │ 4. Worker Pool Consumer Group │
                             │    Command: XREADGROUP        │
                             │    Safety: Pending List (PEL) │
                             └───────────────┬───────────────┘
                                             │
                        ┌────────────────────┴────────────────────┐
                        ▼ (Success)                               ▼ (Failure / Outage)
         ┌───────────────────────────────┐         ┌───────────────────────────────┐
         │ Dispatch to Email / SMS / Web │         │ Calculate Backoff with Jitter │
         │ Command: XACK (Message Done)  │         │ Formula: random(0, 2^n * 1s)  │
         └───────────────────────────────┘         └───────────────┬───────────────┘
                                                                   │
                                                ┌──────────────────┴──────────────────┐
                                                ▼ (Attempts < 3)                      ▼ (Attempts >= 3)
                                 ┌───────────────────────────────┐     ┌───────────────────────────────┐
                                 │ Requeue to Main Stream        │     │ Move to DLQ (Poison Pill)     │
                                 │ Target: notifications:stream  │     │ Stream: notifications:dlq     │
                                 └───────────────────────────────┘     └───────────────────────────────┘
```

---

# 2. The 5 Core Distributed Pillars

### Pillar 1: Distributed Sliding Window Rate Limiting (Redis Lua)
* **The Problem:** In-memory rate limiters fail across horizontal server clusters. Traditional Redis `GET` then `INCR` commands suffer from Time-of-Check to Time-of-Use (TOCTOU) race conditions.
* **The Solution:** A Redis Lua script executing atomically inside Redis memory using Sorted Sets (`ZSET`). Old timestamps are evicted (`ZREMRANGEBYSCORE`), active events are counted (`ZCARD`), and new timestamps are added (`ZADD`) in microsecond time without client locks.

### Pillar 2: Two-Phase Idempotency & Deduplication
* **The Problem:** Networks are at-least-once. Network timeouts lead mobile clients or payment systems to retry identical requests, causing duplicate messages or duplicate charges.
* **The Solution:** The client sends an `Idempotency-Key` header. The server performs an atomic `SET key "PROCESSING" NX EX 30`. If set succeeds, the request is processed and transitioned to `COMPLETED:<response>` (24h TTL). If the key already exists as `COMPLETED`, the cached response is returned immediately without queueing a second dispatch.

### Pillar 3: Asynchronous Queueing with Redis Streams
* **The Problem:** Third-party APIs (Twilio, SendGrid) take 500ms–2000ms to respond and suffer outages. Waiting synchronously exhausts Tomcat's thread pool and crashes the server.
* **The Solution:** The Ingestion API buffers events into a durable append-only log using Redis Streams (`XADD`) and immediately returns `202 Accepted` in `< 5ms`.

### Pillar 4: Consumer Groups, Pending Entries List (PEL), & DLQ
* **The Problem:** If a worker crashes mid-delivery, the message must not be lost. If an external API has a 10-minute outage, blind retries cause a synchronized "thundering herd."
* **The Solution:** Workers pull tasks via `XREADGROUP`. Messages remain tracked in the Redis Pending Entries List (PEL) until explicitly acknowledged with `XACK`. Failed dispatches are retried using **Full Jitter Exponential Backoff**. Messages that fail 3 consecutive times are isolated into `notifications:dlq` (Dead-Letter Queue) to avoid blocking the pipeline.

### Pillar 5: Production Stress Testing with k6
* **The Goal:** Prove that the architecture can sustain thousands of requests per second under heavy concurrency with zero data loss and sub-50ms ingestion latency.

---

# 3. Core Keyword & Annotation Glossary

### Java Language & Concurrency Keywords
* **`record` (Java 16+):** An immutable, final data carrier class. The compiler automatically produces private final fields, a canonical constructor, accessors (`userId()`), `equals()`, `hashCode()`, and `toString()`. Used for DTOs like `NotificationRequest`.
* **`final`:** When applied to fields, guarantees immutability and thread safety once initialized. When applied to classes, prevents subclassing.
* **`ThreadLocalRandom.current()`:** A high-performance random number generator isolated per thread. Avoids the global lock contention inherent in `java.util.Random` under multi-threaded concurrency.
* **`ProcessHandle.current().pid()`:** Java 9+ API to dynamically fetch the Operating System Process ID. Used to uniquely name consumer worker instances (`worker-11168`).
* **`Thread.currentThread().interrupt()`:** Restores the thread's interrupted status after catching an `InterruptedException`, preventing swallowed interruption signals.

### Spring Framework Annotations
* **`@SpringBootApplication`:** A composite annotation combining `@Configuration` (declares beans), `@EnableAutoConfiguration` (configures beans from classpath), and `@ComponentScan` (scans packages for Spring components).
* **`@RestController`:** Combines `@Controller` and `@ResponseBody`. Tells Spring MVC that methods return domain objects serialized directly into HTTP response bodies (JSON via Jackson).
* **`@RequestMapping("/api/v1/notify")`:** Maps incoming web requests to specific controller methods.
* **`@PostMapping`:** Shortcut for `@RequestMapping(method = RequestMethod.POST)`.
* **`@RequestHeader(value = "Idempotency-Key", required = false)`:** Binds an HTTP header directly to a method parameter.
* **`@RequestBody`:** Deserializes the incoming JSON HTTP request body into a strongly typed Java object via Jackson `ObjectMapper`.
* **`@Service`:** Specialization of `@Component` that designates the class as part of the business/service layer. Spring registers it as an injectable singleton.
* **`@Value("${app.property.name:defaultValue}")`:** Injects configuration values from `application.properties` with fallback defaults.
* **`@PostConstruct`:** Executed exactly once after dependency injection is complete. Used to pre-compile Lua scripts or initialize consumer groups before incoming traffic arrives.
* **`@Scheduled(fixedDelay = 50)`:** Tells the Spring task scheduler to invoke the method repeatedly with a fixed duration (50ms) between the completion of the last run and the start of the next.
* **`@EnableScheduling`:** Configured on the main class to activate Spring's background task executor.
* **`@Bean`:** Declares a method as a factory for a Spring-managed singleton bean in the `ApplicationContext`.

### Redis Commands & Primitives
* **`EVALSHA <sha1> <numkeys> <keys...> <args...>`:** Executes a pre-compiled Lua script cached in Redis by its SHA1 hash, eliminating repeated network transfer of raw script text.
* **`ZREMRANGEBYSCORE key min max`:** Removes all elements in a Sorted Set whose score falls within the given range. Used to purge timestamps older than the sliding window.
* **`ZCARD key`:** Returns the cardinality (count) of a Sorted Set in $O(1)$ time.
* **`ZADD key score member`:** Inserts an element with a numeric score into a Sorted Set in $O(\log N)$ time.
* **`PEXPIRE key milliseconds`:** Sets a millisecond Time-To-Live on a Redis key to prevent memory leaks from inactive users.
* **`SET key value NX EX seconds`:** Sets a key if and only if it does not already exist (`NX`), while atomically setting a TTL (`EX`). This is the foundation of distributed locks and idempotency.
* **`XADD key * field value [field value ...]`:** Appends an entry to a Redis Stream with an auto-generated timestamp-based ID (`1715000-0`).
* **`XGROUP CREATE stream group 0 MKSTREAM`:** Creates a consumer group bound to a stream starting from message offset `0`, creating the stream if it does not exist (`MKSTREAM`).
* **`XREADGROUP GROUP group consumer COUNT n BLOCK ms STREAMS stream >`:** Reads unconsumed messages targeting a consumer group. The `>` character instructs Redis to deliver messages never before delivered to any worker in this group.
* **`XACK stream group recordId`:** Acknowledges successful processing of a stream message, removing it from the Pending Entries List (PEL).

---

# 4. Complete Source Code Repository

### 4.1 Redis Lua Rate Limiter
**File:** `src/main/resources/sliding_window.lua`

```lua
-- KEYS[1]: The rate limit key (e.g., "ratelimit:user:user_42")
-- ARGV[1]: Current epoch timestamp in milliseconds
-- ARGV[2]: Window size in milliseconds (e.g., 60000 for 1 minute)
-- ARGV[3]: Max requests allowed in that window (e.g., 100)

local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

-- 1. Calculate the cutoff boundary: any event before this is expired
local clear_before = now - window

-- 2. Evict expired entries
redis.call('ZREMRANGEBYSCORE', key, 0, clear_before)

-- 3. Count remaining entries in the sliding window
local current_requests = redis.call('ZCARD', key)

-- 4. Check quota
if current_requests < limit then
    -- Unique member prevents collisions if multiple requests arrive in the exact same millisecond
    local unique_member = now .. ':' .. redis.call('INCR', key .. ':seq')
    redis.call('ZADD', key, now, unique_member)

    -- Set TTL on keys to prevent RAM leaks
    redis.call('PEXPIRE', key, window)
    redis.call('PEXPIRE', key .. ':seq', window)

    -- Return {allowed = 1, remaining = limit - current - 1}
    return {1, limit - current_requests - 1}
else
    -- Return {allowed = 0, remaining = 0}
    return {0, 0}
end
```

---

### 4.2 Rate Limiter Service
**File:** `src/main/java/com/system/notification_engine/model/RateLimitResult.java`

```java
package com.system.notification_engine.model;

/**
 * Immutable carrier for rate limit decisions.
 */
public record RateLimitResult(boolean allowed, long remainingQuota) {
}
```

**File:** `src/main/java/com/system/notification_engine/ratelimiter/RateLimiterService.java`

```java
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
            // Fail open: don't block user traffic if rate limit infrastructure hiccups
            return new RateLimitResult(true, 0);
        }
    }
}
```

---

### 4.3 Idempotency Service
**File:** `src/main/java/com/system/notification_engine/model/IdempotencyResult.java`

```java
package com.system.notification_engine.model;

public record IdempotencyResult(Status status, String cachedResponse) {

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
```

**File:** `src/main/java/com/system/notification_engine/idempotency/IdempotencyService.java`

```java
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

        // Atomic lock attempt
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
```

---

### 4.4 Domain Models
**File:** `src/main/java/com/system/notification_engine/model/ChannelType.java`

```java
package com.system.notification_engine.model;

public enum ChannelType {
    EMAIL,
    SMS,
    WEBHOOK
}
```

**File:** `src/main/java/com/system/notification_engine/model/NotificationRequest.java`

```java
package com.system.notification_engine.model;

public record NotificationRequest(
    String userId,
    ChannelType channel,
    String recipient,
    String content
) {
}
```

**File:** `src/main/java/com/system/notification_engine/model/NotificationResponse.java`

```java
package com.system.notification_engine.model;

public record NotificationResponse(
        String notificationId,
        String status,
        String message
) {
}
```

---

### 4.5 Queue Producer
**File:** `src/main/java/com/system/notification_engine/queue/NotificationQueueProducer.java`

```java
package com.system.notification_engine.queue;

import com.system.notification_engine.model.NotificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class NotificationQueueProducer {

    private static final Logger log = LoggerFactory.getLogger(NotificationQueueProducer.class);

    private final StringRedisTemplate redisTemplate;

    @Value("${app.queue.stream-name:notifications:stream}")
    private String streamName;

    public NotificationQueueProducer(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public String enqueue(NotificationRequest request) {
        String trackingId = UUID.randomUUID().toString();

        Map<String, String> message = new HashMap<>();
        message.put("trackingId", trackingId);
        message.put("userId", request.userId());
        message.put("channel", request.channel().name());
        message.put("recipient", request.recipient());
        message.put("content", request.content());
        message.put("timestamp", String.valueOf(System.currentTimeMillis()));
        message.put("retryCount", "0");

        StringRecord record = StreamRecords.string(message).withStreamKey(streamName);
        RecordId recordId = redisTemplate.opsForStream().add(record);

        log.debug("Enqueued notification with trackingId {} to stream {}: {}", trackingId, streamName, recordId);
        return trackingId;
    }
}
```

---

### 4.6 Channel Dispatcher Mock
**File:** `src/main/java/com/system/notification_engine/worker/NotificationDispatcher.java`

```java
package com.system.notification_engine.worker;

import com.system.notification_engine.model.ChannelType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;

@Service
public class NotificationDispatcher {

    private static final Logger log = LoggerFactory.getLogger(NotificationDispatcher.class);

    public void dispatch(String trackingId, ChannelType channel, String recipient, String content) {
        // Simulate external provider I/O latency
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(10, 30));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Simulate 5% transient failure rate to verify retry & DLQ pipeline
        int randomRoll = ThreadLocalRandom.current().nextInt(100);
        if (randomRoll < 5) {
            log.warn("Downstream provider 500 error for channel {} (trackingId: {})", channel, trackingId);
            throw new RuntimeException("Downstream " + channel + " provider unavailable (Simulated 503/500)");
        }

        log.info("DISPATCH SUCCESS [{}] to {} for trackingId: {}", channel, recipient, trackingId);
    }
}
```

---

### 4.7 Exponential Backoff with Full Jitter
**File:** `src/main/java/com/system/notification_engine/worker/BackoffUtil.java`

```java
package com.system.notification_engine.worker;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Implements Exponential Backoff with Full Jitter.
 * Formula: sleep = random(0, min(MaxCap, Base * 2^attempt))
 */
public class BackoffUtil {

    private static final long BASE_DELAY_MS = 1000;  // 1 second
    private static final long MAX_DELAY_MS = 30000;  // 30 seconds

    public static long calculateJitteredBackoff(int retryCount) {
        long exponentialDelay = BASE_DELAY_MS * (1L << Math.min(retryCount, 10));
        long cappedDelay = Math.min(exponentialDelay, MAX_DELAY_MS);
        return ThreadLocalRandom.current().nextLong(0, cappedDelay + 1);
    }
}
```

---

### 4.8 Stream Consumer & DLQ Engine
**File:** `src/main/java/com/system/notification_engine/worker/NotificationStreamConsumer.java`

```java
package com.system.notification_engine.worker;

import com.system.notification_engine.model.ChannelType;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class NotificationStreamConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationStreamConsumer.class);

    private final StringRedisTemplate redisTemplate;
    private final NotificationDispatcher dispatcher;

    @Value("${app.queue.stream-name:notifications:stream}")
    private String streamName;

    @Value("${app.queue.consumer-group:notification-workers}")
    private String consumerGroup;

    private static final String DLQ_STREAM = "notifications:dlq";
    private static final String CONSUMER_NAME = "worker-" + ProcessHandle.current().pid();
    private static final int MAX_RETRIES = 3;

    public NotificationStreamConsumer(StringRedisTemplate redisTemplate, NotificationDispatcher dispatcher) {
        this.redisTemplate = redisTemplate;
        this.dispatcher = dispatcher;
    }

    @PostConstruct
    public void initConsumerGroup() {
        try {
            redisTemplate.opsForStream().createGroup(streamName, ReadOffset.from("0"), consumerGroup);
            log.info("Consumer group '{}' created for stream '{}'", consumerGroup, streamName);
        } catch (Exception e) {
            log.info("Consumer group '{}' already exists for stream '{}'", consumerGroup, streamName);
        }
    }

    @Scheduled(fixedDelay = 50)
    public void pollMessages() {
        try {
            List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                    Consumer.from(consumerGroup, CONSUMER_NAME),
                    StreamReadOptions.empty().count(20).block(Duration.ofMillis(500)),
                    StreamOffset.create(streamName, ReadOffset.lastConsumed())
            );

            if (records == null || records.isEmpty()) {
                return;
            }

            for (MapRecord<String, Object, Object> record : records) {
                processRecord(record);
            }
        } catch (Exception e) {
            log.error("Error polling Redis stream", e);
        }
    }

    private void processRecord(MapRecord<String, Object, Object> record) {
        RecordId recordId = record.getId();
        Map<Object, Object> value = record.getValue();

        String trackingId = (String) value.get("trackingId");
        ChannelType channel = ChannelType.valueOf((String) value.get("channel"));
        String recipient = (String) value.get("recipient");
        String content = (String) value.get("content");
        int retryCount = Integer.parseInt(value.getOrDefault("retryCount", "0").toString());

        try {
            dispatcher.dispatch(trackingId, channel, recipient, content);
            redisTemplate.opsForStream().acknowledge(streamName, consumerGroup, recordId);
        } catch (Exception dispatchException) {
            log.warn("Dispatch failed for trackingId: {} (Attempt {}/{})", trackingId, retryCount + 1, MAX_RETRIES);

            if (retryCount + 1 >= MAX_RETRIES) {
                routeToDlq(value, dispatchException.getMessage());
                redisTemplate.opsForStream().acknowledge(streamName, consumerGroup, recordId);
            } else {
                retryMessage(value, retryCount + 1);
                redisTemplate.opsForStream().acknowledge(streamName, consumerGroup, recordId);
            }
        }
    }

    private void retryMessage(Map<Object, Object> value, int nextRetryCount) {
        long backoffMs = BackoffUtil.calculateJitteredBackoff(nextRetryCount);
        log.info("Scheduling retry #{} for trackingId {} after {} ms delay",
                nextRetryCount, value.get("trackingId"), backoffMs);

        Map<String, String> retryMap = new HashMap<>();
        value.forEach((k, v) -> retryMap.put(k.toString(), v.toString()));
        retryMap.put("retryCount", String.valueOf(nextRetryCount));

        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        redisTemplate.opsForStream().add(StreamRecords.string(retryMap).withStreamKey(streamName));
    }

    private void routeToDlq(Map<Object, Object> value, String failureReason) {
        log.error("POISON PILL / PERMANENT FAILURE: Moving trackingId {} to DLQ [{}]",
                value.get("trackingId"), DLQ_STREAM);

        Map<String, String> dlqMap = new HashMap<>();
        value.forEach((k, v) -> dlqMap.put(k.toString(), v.toString()));
        dlqMap.put("dlqTimestamp", String.valueOf(System.currentTimeMillis()));
        dlqMap.put("failureReason", failureReason);

        redisTemplate.opsForStream().add(StreamRecords.string(dlqMap).withStreamKey(DLQ_STREAM));
    }
}
```

---

### 4.9 Ingestion Controller
**File:** `src/main/java/com/system/notification_engine/controller/NotificationController.java`

```java
package com.system.notification_engine.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.system.notification_engine.idempotency.IdempotencyService;
import com.system.notification_engine.model.IdempotencyResult;
import com.system.notification_engine.model.NotificationRequest;
import com.system.notification_engine.model.NotificationResponse;
import com.system.notification_engine.model.RateLimitResult;
import com.system.notification_engine.queue.NotificationQueueProducer;
import com.system.notification_engine.ratelimiter.RateLimiterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/notify")
public class NotificationController {

    private static final Logger log = LoggerFactory.getLogger(NotificationController.class);

    private final NotificationQueueProducer notificationQueueProducer;
    private final RateLimiterService rateLimiterService;
    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    @Value("${app.rate-limit.window-size-ms:60000}")
    private long windowSizeMs;

    @Value("${app.rate-limit.max-requests:100}")
    private long maxRequests;

    public NotificationController(
            NotificationQueueProducer notificationQueueProducer,
            RateLimiterService rateLimiterService,
            IdempotencyService idempotencyService,
            ObjectMapper objectMapper
    ) {
        this.notificationQueueProducer = notificationQueueProducer;
        this.rateLimiterService = rateLimiterService;
        this.idempotencyService = idempotencyService;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public ResponseEntity<NotificationResponse> sendNotification(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody NotificationRequest request
    ) throws JsonProcessingException {

        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new NotificationResponse(null, "REJECTED", "Missing required 'Idempotency-Key' header"));
        }

        // 1. Distributed Rate Limiting
        String rateLimitKey = "ratelimit:user:" + request.userId();
        RateLimitResult rateLimitResult = rateLimiterService.isAllowed(rateLimitKey, windowSizeMs, maxRequests);

        if (!rateLimitResult.allowed()) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("X-RateLimit-Remaining", "0")
                    .header("Retry-After", String.valueOf(windowSizeMs / 1000))
                    .body(new NotificationResponse(null, "RATE_LIMITED", "Rate limit exceeded. Try again later."));
        }

        // 2. Idempotency Lock
        IdempotencyResult idempotencyResult = idempotencyService.tryAcquire(idempotencyKey);

        if (idempotencyResult.status() == IdempotencyResult.Status.CONFLICT) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(new NotificationResponse(null, "IN_FLIGHT", "Idempotency key is already being processed"));
        }

        if (idempotencyResult.status() == IdempotencyResult.Status.ALREADY_COMPLETED) {
            NotificationResponse cachedResponse = objectMapper.readValue(
                    idempotencyResult.cachedResponse(),
                    NotificationResponse.class
            );
            return ResponseEntity.ok(cachedResponse);
        }

        // 3. Queue Buffer
        String notificationId = notificationQueueProducer.enqueue(request);

        NotificationResponse response = new NotificationResponse(
                notificationId,
                "QUEUED",
                "Notification accepted for delivery."
        );

        // 4. Mark Completed with Cached JSON
        idempotencyService.markCompleted(idempotencyKey, objectMapper.writeValueAsString(response));

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .header("X-RateLimit-Remaining", String.valueOf(rateLimitResult.remainingQuota()))
                .body(response);
    }
}
```

---

### 4.10 Main Application
**File:** `src/main/java/com/system/notification_engine/NotificationEngineApplication.java`

```java
package com.system.notification_engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class NotificationEngineApplication {

	public static void main(String[] args) {
		SpringApplication.run(NotificationEngineApplication.class, args);
	}

	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}
}
```

---

### 4.11 Configuration Files
**File:** `src/main/resources/application.properties`

```properties
spring.application.name=notification-engine
server.port=8080

# Redis Connection & Lettuce Pool
spring.data.redis.host=localhost
spring.data.redis.port=6379
spring.data.redis.timeout=2000ms
spring.data.redis.lettuce.pool.max-active=50
spring.data.redis.lettuce.pool.max-idle=20
spring.data.redis.lettuce.pool.min-idle=5
spring.data.redis.lettuce.pool.max-wait=1000ms

# App Tuning
app.rate-limit.window-size-ms=60000
app.rate-limit.max-requests=100
app.queue.stream-name=notifications:stream
app.queue.consumer-group=notification-workers
```

**File:** `build.gradle`

```groovy
plugins {
	id 'java'
	id 'org.springframework.boot' version '4.1.2'
	id 'io.spring.dependency-management' version '1.1.7'
}

group = 'com.system'
version = '0.0.1-SNAPSHOT'

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(25)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation 'org.springframework.boot:spring-boot-starter-actuator'
	implementation 'org.springframework.boot:spring-boot-starter-data-redis'
	implementation 'org.apache.commons:commons-pool2'
	implementation 'org.springframework.boot:spring-boot-starter-webmvc'
	implementation 'com.fasterxml.jackson.core:jackson-databind'
	testImplementation 'org.springframework.boot:spring-boot-starter-actuator-test'
	testImplementation 'org.springframework.boot:spring-boot-starter-data-redis-test'
	testImplementation 'org.springframework.boot:spring-boot-starter-webmvc-test'
	testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}

tasks.named('test') {
	useJUnitPlatform()
}
```

---

### 4.12 Load Test Benchmark Script
**File:** `load_test.js`

```javascript
import http from 'k6/http';
import { check } from 'k6';

function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

export const options = {
    scenarios: {
        burst_test: {
            executor: 'ramping-arrival-rate',
            startRate: 100,
            timeUnit: '1s',
            preAllocatedVUs: 50,
            maxVUs: 500,
            stages: [
                { duration: '10s', target: 500 },
                { duration: '20s', target: 2000 },
                { duration: '10s', target: 0 },
            ],
        },
    },
    thresholds: {
        'http_req_duration': ['p(95)<50', 'p(99)<100'],
        'http_req_failed': ['rate<0.05'],
    },
};

export default function () {
    const randomUser = `user_${Math.floor(Math.random() * 500)}`;

    const payload = JSON.stringify({
        userId: randomUser,
        channel: 'EMAIL',
        recipient: `${randomUser}@enterprise.com`,
        content: 'Your transaction was approved.',
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': generateUUID(),
        },
    };

    const res = http.post('http://localhost:8080/api/v1/notify', payload, params);

    check(res, {
        'status is 202 Accepted': (r) => r.status === 202,
    });
}
```

---

# 5. Step-by-Step Request Lifecycle & Trace

### Scenario A: Clean Ingestion (First Time Request)
1. **Client sends:** `POST /api/v1/notify` with `Idempotency-Key: uuid-100` and `userId: user_42`.
2. **Controller:** Validates header presence $\rightarrow$ calls `RateLimiterService`.
3. **Redis Lua Script:** Purges items older than 60s $\rightarrow$ counts active items $\rightarrow$ adds `now:uuid-100` to Sorted Set $\rightarrow$ returns `[1, remainingQuota = 99]`.
4. **Idempotency Service:** Executes `SET IDEMPOTENCY:uuid-100 "PROCESSING" NX EX 30`. Returns `ACQUIRED`.
5. **Queue Producer:** Executes `XADD notifications:stream * trackingId <uuid> userId user_42 ...`.
6. **Idempotency Service:** Updates `IDEMPOTENCY:uuid-100` to `COMPLETED:{"notificationId":"<uuid>","status":"QUEUED"...}` with a 24-hour TTL.
7. **Controller returns:** `HTTP 202 Accepted` to client with header `X-RateLimit-Remaining: 99`.
8. **Worker:** In background, `NotificationStreamConsumer` reads message via `XREADGROUP` $\rightarrow$ dispatches to channel $\rightarrow$ acknowledges with `XACK`.

### Scenario B: Client Network Retry (Duplicate Request)
1. **Client sends:** The identical `POST /api/v1/notify` with `Idempotency-Key: uuid-100`.
2. **Rate Limiter:** Checks quota $\rightarrow$ allowed.
3. **Idempotency Service:** Executes `SET IDEMPOTENCY:uuid-100 "PROCESSING" NX EX 30`.
4. **Redis:** Returns `null/false` (key already exists).
5. **Idempotency Service:** Reads `GET IDEMPOTENCY:uuid-100` $\rightarrow$ detects prefix `COMPLETED:`.
6. **Controller:** Deserializes the cached JSON response and immediately returns `HTTP 200 OK`.
7. **Zero events enqueued. Zero duplicate dispatches.**

---

# 6. Concurrency Hazards & Failure Modes Resolved

### 1. TOCTOU Race Conditions
* *Hazard:* Thread A checks quota ($N=99$), Thread B checks quota ($N=99$), both proceed to increment, allowing $101$ requests past a $100$-limit threshold.
* *Resolution:* Redis executes the Lua script atomically on a single thread. Evaluation and insertion happen in one uninterrupted operation.

### 2. Lock Leaks on Server Crash
* *Hazard:* Server marks idempotency key as `PROCESSING`, but crashes before completing. If no TTL is set, client is permanently blocked from retrying.
* *Resolution:* Lock acquisition uses `NX EX 30`. If the node dies, the lock expires automatically in 30 seconds.

### 3. Thundering Herd Problem
* *Hazard:* An external provider experiences a transient 500 outage. 5,000 workers fail simultaneously and all retry at the exact same exponential interval ($t+1$s, $t+2$s), sending synchronized waves that knock the recovering service down.
* *Resolution:* **Full Jitter** randomizes delays between $0$ and $\min(\text{MaxCap}, \text{Base} \cdot 2^{\text{attempt}})$, smoothing the retry distribution across time.

### 4. Poison Pills Blocking the Stream
* *Hazard:* A malformed message payload causes an unhandled parser exception or persistent external 400 rejection, looping infinitely and blocking the queue.
* *Resolution:* After 3 failed attempts, the consumer isolates the payload into `notifications:dlq` and sends `XACK` on the main stream, preserving pipeline flow.

---

# 7. Empirical Benchmarks & Capacity Math

During live stress testing with `k6`:
* **Total Ingested Requests:** 24,697 requests.
* **Failure Rate:** **0.00%** (24,697 / 24,697 returned `202 Accepted`).
* **Ingestion Throughput:** Peaked at over **1,200 requests/sec** on a single laptop node.
* **Stream Durability:** Verified 24,800 messages safely retained in `notifications:stream`.

### Concurrency Scaling Math:
$$\text{Ingestion Latency} \approx \text{Lua Roundtrip (1ms)} + \text{SETNX (1ms)} + \text{XADD (1ms)} = 3\text{ms}$$
With non-blocking I/O and connection pooling (50 Lettuce pooled sockets), a single Spring Boot node easily handles:
$$\text{Max Theoretical RPS} = \frac{50 \text{ connections}}{0.003 \text{ seconds}} \approx 16,666 \text{ RPS}$$

---

# 8. Interview Drill-Down Questions & Model Answers

### Q1: "Why use a Redis Lua script instead of standard Redis transactions (`MULTI`/`EXEC`)?"
> **Answer:** `MULTI`/`EXEC` in Redis only queues commands; it cannot use the result of one command (e.g. `ZCARD`) to decide whether to execute the next command (`ZADD`) inside the same atomic transaction. Redis Lua scripts allow conditional branching and logic executed atomically on the server in a single network round-trip.

### Q2: "What happens if Redis crashes right after `SETNX` succeeds?"
> **Answer:** Because we use `SETNX` with a 30-second TTL (`NX EX 30`), Redis will automatically evict the key if the node dies or fails to update to `COMPLETED`. The client's subsequent retry after timeout will successfully re-acquire the lock on another healthy server.

### Q3: "How does Redis Streams prevent message loss if a worker crashes?"
> **Answer:** Redis Streams maintains a **Pending Entries List (PEL)** for each consumer group. When a worker reads a message via `XREADGROUP`, the message is marked as pending. Only when the worker completes processing and sends an explicit `XACK` is it removed from the PEL. If a worker terminates, another worker can discover pending messages using `XPENDING` or `XCLAIM` and take over processing.

### Q4: "How do you scale from 3,000 RPS to 50,000+ RPS?"
> **Answer:** Three architectural upgrades:
> 1. Enable **Java 21 Virtual Threads** (`spring.threads.virtual.enabled=true`) to eliminate Tomcat platform thread limits.
> 2. Partition Redis Streams by hash key (e.g., `notifications:stream:{shard_id}`) across a **Redis Cluster**.
> 3. Scale the Ingestion API horizontally as stateless containers behind a Cloud Load Balancer (ALB), with Lettuce connection pools scaled per node.
