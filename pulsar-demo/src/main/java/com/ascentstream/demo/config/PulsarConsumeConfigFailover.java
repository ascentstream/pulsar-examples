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
public class PulsarConsumeConfigFailover {

    private final Logger logger = LoggerFactory.getLogger(PulsarConsumeConfigFailover.class);


    private final CommonConfig commonConfig;
    private final PulsarClient pulsarClient;


    public PulsarConsumeConfigFailover(CommonConfig commonConfig, PulsarClient pulsarClient) {
        this.commonConfig = commonConfig;
        this.pulsarClient = pulsarClient;
    }


    @Bean
    Consumer pulsarFailoverConsumerA() throws PulsarClientException {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(commonConfig.getTestTopic())
                .consumerName("consumerFailoverA")
                .subscriptionName("subscription-Failover")
                .subscriptionType(SubscriptionType.Failover)
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
    Consumer pulsarFailoverConsumerB() throws PulsarClientException {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(commonConfig.getTestTopic())
                .consumerName("consumerFailoverB")
                .subscriptionName("subscription-Failover")
                .subscriptionType(SubscriptionType.Failover)
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

