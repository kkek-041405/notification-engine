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
