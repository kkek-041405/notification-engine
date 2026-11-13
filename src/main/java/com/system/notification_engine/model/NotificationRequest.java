package com.system.notification_engine.model;

public record NotificationRequest(
    String userId,
    ChannelType channel,
    String recipient,
    String content
) {
}
