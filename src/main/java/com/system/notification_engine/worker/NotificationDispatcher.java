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
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(10, 30));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        int randomRoll = ThreadLocalRandom.current().nextInt(100);
        if (randomRoll < 5) {
            log.warn("Downstream provider 500 error for channel {} (trackingId: {})", channel, trackingId);
            throw new RuntimeException("Downstream " + channel + " provider unavailable (Simulated 503/500)");
        }

        log.info("DISPATCH SUCCESS [{}] to {} for trackingId: {}", channel, recipient, trackingId);
    }
}
