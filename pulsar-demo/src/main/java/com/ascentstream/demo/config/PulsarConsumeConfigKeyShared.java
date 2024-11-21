package com.ascentstream.demo.config;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarConsumeConfigKeyShared {

    private static final Logger logger = LoggerFactory.getLogger(PulsarConsumeConfigKeyShared.class);

    private final CommonConfig commonConfig;
    private final PulsarClient pulsarClient;


    public PulsarConsumeConfigKeyShared(CommonConfig commonConfig, PulsarClient pulsarClient) {
        this.commonConfig = commonConfig;
        this.pulsarClient = pulsarClient;
    }

    @Bean
    Consumer pulsarKeySharedConsumerA() throws PulsarClientException {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(commonConfig.getTestTopic())
                .consumerName("consumerKeySharedA")
                .subscriptionName("subscription-KeyShared")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .messageListener(new MessageListener<String>() {
                    @Override
                    public void received(Consumer<String> consumer, Message<String> msg) {
                        logger.info("{} received msg, topic: {}, value: {} ", consumer.getConsumerName(), msg.getTopicName(), msg.getValue());
                    }
                })
                .subscribe();
        return consumer;
    }

    @Bean
    Consumer pulsarKeySharedConsumerB() throws PulsarClientException {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(commonConfig.getTestTopic())
                .consumerName("consumerKeySharedB")
                .subscriptionName("subscription-KeyShared")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .messageListener(new MessageListener<String>() {
                    @Override
                    public void received(Consumer<String> consumer, Message<String> msg) {
                        logger.info("{} received msg, topic: {}, value: {} ", consumer.getConsumerName(), msg.getTopicName(), msg.getValue());
                    }
                })
                .subscribe();
        return consumer;
    }

}

