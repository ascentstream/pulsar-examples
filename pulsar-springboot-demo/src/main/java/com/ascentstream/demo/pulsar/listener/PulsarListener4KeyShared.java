package com.ascentstream.demo.pulsar.listener;

import java.util.List;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.listener.AckMode;
import org.springframework.stereotype.Component;


@Component
public class PulsarListener4KeyShared {
    
    private static final Logger logger = LoggerFactory.getLogger(PulsarListener4KeyShared.class);


    // Subscription Key_Shared demo
    @PulsarListener(
            schemaType = SchemaType.STRING,
            subscriptionName = "subscription-KeyShared",
            subscriptionType = SubscriptionType.Key_Shared,
            ackMode = AckMode.MANUAL,
            topics = {"${pulsar.topic.test-topic: test-topic}"},
            autoStartup = "true",
            batch = true,
            properties = { "consumerName=consumerKeySharedA" }
    )
    public void listen4KeySharedA(List<Message<String>> messages, Consumer<String> consumer) {
        logger.info("consumer {} received messages, size: {}", consumer.getConsumerName(), messages.size());
        messages.forEach((message) -> {
            logger.info("MessageKey: {}, MessageValue: {}", message.getKey(), message.getValue());
            try {
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @PulsarListener(
            schemaType = SchemaType.STRING,
            subscriptionName = "subscription-KeyShared",
            subscriptionType = SubscriptionType.Key_Shared,
            ackMode = AckMode.MANUAL,
            topics = {"${pulsar.topic.test-topic: test-topic}"},
            autoStartup = "true",
            batch = true,
            properties = { "consumerName=consumerKeySharedB" }

    )
    public void listen4KeySharedB(List<Message<String>> messages, Consumer<String> consumer) {
        logger.info("consumer {} received messages, size: {}", consumer.getConsumerName(), messages.size());
        messages.forEach((message) -> {
            logger.info("MessageKey: {}, MessageValue: {}", message.getKey(), message.getValue());
            try {
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
