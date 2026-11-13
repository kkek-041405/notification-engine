package com.system.notification_engine.model;




public record NotificationResponse(
    String notificationId,
    String status,
    String message
) {
}