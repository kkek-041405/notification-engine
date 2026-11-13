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

        String rateLimitKey = "ratelimit:user:" + request.userId();
        RateLimitResult rateLimitResult = rateLimiterService.isAllowed(rateLimitKey, windowSizeMs, maxRequests);

        if (!rateLimitResult.allowed()) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("X-RateLimit-Remaining", "0")
                    .header("Retry-After", String.valueOf(windowSizeMs / 1000))
                    .body(new NotificationResponse(null, "RATE_LIMITED", "Rate limit exceeded. Try again later."));
        }

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

        String notificationId = notificationQueueProducer.enqueue(request);

        NotificationResponse response = new NotificationResponse(
                notificationId,
                "QUEUED",
                "Notification accepted for delivery."
        );

        idempotencyService.markCompleted(idempotencyKey, objectMapper.writeValueAsString(response));

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .header("X-RateLimit-Remaining", String.valueOf(rateLimitResult.remainingQuota()))
                .body(response);
    }
}
