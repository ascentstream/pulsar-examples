package com.ascentstream.demo.pulsar.listener;

import java.io.IOException;
import java.util.List;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.listener.AckMode;
import org.springframework.stereotype.Component;


@Component
public class PulsarListener4Exclusive {

    private static final Logger logger = LoggerFactory.getLogger(PulsarListener4Exclusive.class);


    // Subscription Exclusive demo
    @PulsarListener(
            schemaType = SchemaType.STRING,
            subscriptionName = "subscription-Exclusive",
            subscriptionType = SubscriptionType.Exclusive,
            ackMode = AckMode.MANUAL,
            topics = {"${pulsar.topic.test-topic: test-topic}"},
            autoStartup = "true",
            batch = true,
            properties = { "consumerName=consumerExclusiveA" },
            consumerCustomizer = "consumerBatchReceiveCustomizer"
    )
    public void listen4ExclusiveA(List<Message<String>> messages, Consumer<String> consumer) {
        logger.info("consumer {} received messages, size: {}", consumer.getConsumerName(), messages.size());
        messages.forEach((message) -> {
            try {
                consumer.acknowledge(message);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        });
    }

    @PulsarListener(
            schemaType = SchemaType.STRING,
            subscriptionName = "subscription-Exclusive",
            subscriptionType = SubscriptionType.Exclusive,
            ackMode = AckMode.MANUAL,
            topics = {"${pulsar.topic.test-topic: test-topic}"},
            // If autoStartup=true, in Exclusive subscription mode, will get errorMsg "Exclusive consumer is already connected." during app booting
            autoStartup = "false",
            batch = true,
            properties = { "consumerName=consumerExclusiveB" }
    )
    public void listen4ExclusiveB(List<Message<String>> messages, Consumer<String> consumer) {
        logger.info("consumer {} received messages, size: {}", consumer.getConsumerName(), messages.size());
        messages.forEach((message) -> {
            try {
                consumer.acknowledge(message);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
