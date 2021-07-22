package com.tutuka.redis;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.tutuka.pubsubclient.PubSubClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

@Component
public class PubSubConsumer implements Consumer {

    private final static Logger LOG = LoggerFactory.getLogger(PubSubConsumer.class);

    private PubSubClient PubSubClient;

    private final ListOperations redis;

    private final String queueName;

    private final ExponentialBackOff exponentialBackOff;

    private static final int MAX_ATTEMPTS = 10;

    private static final int TRANSACTION_DAEMON_FREQUENCY_MS = 30;

    @Autowired
    public PubSubConsumer(ListOperations redis,
                          PubSubClient PubSubClient,
                          @Value("${queue_name}") String queueName) {
        this.redis = redis;
        this.PubSubClient = PubSubClient;
        this.queueName = queueName;
        this.exponentialBackOff = new ExponentialBackOff();
    }

    @Override
    @Scheduled(fixedDelay = TRANSACTION_DAEMON_FREQUENCY_MS)
    public void consume() {
        LOG.debug("PubSubConsumer.consume() started");
        String message = null;
        try {
            message = (String) redis.rightPop(queueName);
        } catch (Exception e) {
            LOG.error("Can't pop message from Redis", e);
        }

        if (message != null) {
            LOG.debug("message: " + message);
            JsonParser jsonParser = new JsonParser();
            JsonElement jsonElement = jsonParser.parse(message);
            JsonElement jsonPayload = jsonElement.getAsJsonObject().get("PAYLOAD");
            JsonElement jsonChannel = jsonElement.getAsJsonObject().get("CHANNEL");

            if (jsonPayload == null) {
                LOG.error("Payload is missing");
                return;
            }

            if (jsonChannel == null) {
                LOG.error("Channel is missing");
                return;
            }

            String payload = jsonPayload.toString();
            String channel = jsonChannel.getAsString();

            LOG.debug("Before Publishing message");

            // Before sending another message we need to be sure this one has been published
            boolean isPublished = publishMessage(message, payload, channel);

            if (!isPublished) {
                pushBackToRedis(message);
            }
        }
    }

    private boolean publishMessage(String message, String payload, String channel) {
        boolean result = false;
        BackOffExecution execution = exponentialBackOff.start();
        for (int i = 0; i < MAX_ATTEMPTS; i++) {
            try {
                LOG.info("Publishing message: " + payload);
                String resp = PubSubClient.publish(payload, channel);
                LOG.debug("Message: " + resp);
                return true;
            } catch (Exception e) {
                long pause = execution.nextBackOff();
                LOG.error("Can't publish message to PubSub, reconnecting PubSub client", e);
                LOG.error("Queue size is:" + redis.size(queueName));
                LOG.info("Next attempt in " + pause + "ms");
                try {
                    Thread.sleep(pause);
                } catch (InterruptedException e1) {
                    LOG.error("Error on Thread.sleep()", e1);
                }
            }
        }
        return result;
    }

    private void pushBackToRedis(String message) {
        String subject = "Failed to publish message: " + message;
        LOG.error(subject);
        try {
            redis.leftPush(queueName, message);
        } catch (Exception e) {
            String pushFailedSubject = "Failed to push message back to com.tutuka.redis queue";
            LOG.error(pushFailedSubject);
        }
    }

}
