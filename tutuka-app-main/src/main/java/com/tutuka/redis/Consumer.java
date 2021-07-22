package com.tutuka.redis;

import org.springframework.scheduling.annotation.Scheduled;

public interface Consumer {
    @Scheduled(fixedDelay = 100)
    void consume();
}
