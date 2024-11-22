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
public class PulsarListener4Shared {
    
    private static final Logger logger = LoggerFactory.getLogger(PulsarListener4Shared.class);


    // Subscription Shared demo
    @PulsarListener(
            schemaType = SchemaType.STRING,
            subscriptionName = "subscription-Shared",
            subscriptionType = SubscriptionType.Shared,
            ackMode = AckMode.MANUAL,
            topics = {"${pulsar.topic.test-topic: test-topic}"},
            autoStartup = "false",
            batch = true,
            properties = { "consumerName=consumerSharedA" },
            consumerCustomizer = "consumerBatchReceiveCustomizer"
    )
    public void listen4SharedA(List<Message<String>> messages, Consumer<String> consumer) {
        logger.info("consumer {} received messages, size: {}", consumer.getConsumerName(), messages.size());
        messages.forEach((message) -> {
            logger.info(message.getValue());
            consumer.negativeAcknowledge(message);
        });
    }

    @PulsarListener(
            schemaType = SchemaType.STRING,
            subscriptionName = "subscription-Shared",
            subscriptionType = SubscriptionType.Shared,
            ackMode = AckMode.MANUAL,
            topics = {"${pulsar.topic.test-topic: test-topic}"},
            autoStartup = "true",
            batch = true,
            properties = { "consumerName=consumerSharedB" },
            consumerCustomizer = "consumerDeadCustomizer"
//            consumerCustomizer = "consumerRetryCustomizer"
    )
    public void listen4SharedB(List<Message<String>> messages, Consumer<String> consumer) {
        logger.info("consumer {} received messages, size: {}", consumer.getConsumerName(), messages.size());
        messages.forEach((message) -> {
            logger.info(message.getValue());
            int a = 1 / 0;
            consumer.negativeAcknowledge(message);
        });
    }

    @PulsarListener(
            schemaType = SchemaType.STRING,
            subscriptionName = "subscription-Shared",
            subscriptionType = SubscriptionType.Shared,
            ackMode = AckMode.MANUAL,
            topics = {"test-topic-dead"},
            autoStartup = "true",
            batch = true,
            properties = { "consumerName=consumerSharedB4Dead" }
    )
    public void listen4SharedB4Dead(List<Message<String>> messages, Consumer<String> consumer) {
        logger.info("consumer {} received messages, size: {}", consumer.getConsumerName(), messages.size());
        messages.forEach((message) -> {
            logger.info(message.getValue());
            try {
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
