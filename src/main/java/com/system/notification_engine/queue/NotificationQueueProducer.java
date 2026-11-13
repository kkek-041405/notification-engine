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
