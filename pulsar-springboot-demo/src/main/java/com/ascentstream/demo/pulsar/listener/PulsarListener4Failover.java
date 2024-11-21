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
public class PulsarListener4Failover {
    
    private static final Logger logger = LoggerFactory.getLogger(PulsarListener4Failover.class);


    // Subscription Failover demo
    @PulsarListener(
            schemaType = SchemaType.STRING,
            subscriptionName = "subscription-Failover",
            subscriptionType = SubscriptionType.Failover,
            ackMode = AckMode.MANUAL,
            topics = {"${pulsar.topic.test-topic: test-topic}"},
            autoStartup = "true",
            batch = true,
            properties = { "consumerName=consumerFailoverA" }
    )
    public void listen4FailoverA(List<Message<String>> messages, Consumer<String> consumer) {
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

    @PulsarListener(
            schemaType = SchemaType.STRING,
            subscriptionName = "subscription-Failover",
            subscriptionType = SubscriptionType.Failover,
            ackMode = AckMode.MANUAL,
            topics = {"${pulsar.topic.test-topic: test-topic}"},
            autoStartup = "true",
            batch = true,
            properties = { "consumerName=consumerFailoverB" }
    )
    public void listen4FailoverB(List<Message<String>> messages, Consumer<String> consumer) {
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
