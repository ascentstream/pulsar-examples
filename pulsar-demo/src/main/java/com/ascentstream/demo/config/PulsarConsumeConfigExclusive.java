package com.ascentstream.demo.config;

import java.io.IOException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
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
public class PulsarConsumeConfigExclusive {

    private final Logger logger = LoggerFactory.getLogger(PulsarConsumeConfigExclusive.class);


    private final CommonConfig commonConfig;
    private final PulsarClient pulsarClient;


    public PulsarConsumeConfigExclusive(CommonConfig commonConfig, PulsarClient pulsarClient) {
        this.commonConfig = commonConfig;
        this.pulsarClient = pulsarClient;
    }

//    @Bean
    Consumer pulsarExclusiveConsumerA() throws PulsarClientException {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(commonConfig.getTestTopic())
                .consumerName("consumerExclusiveA")
                .subscriptionName("subscription-Exclusive")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionMode(SubscriptionMode.Durable)
                .intercept(new ConsumerInterceptor[]{})
                .messageListener(new MessageListener<String>() {
                    @Override
                    public void received(Consumer<String> consumer, Message<String> msg) {
                        logger.info("{} received msg, topic: {}, value: {} ", consumer.getConsumerName(), msg.getTopicName(), msg.getValue());
                        try {
                            consumer.acknowledge(msg);
                        } catch (PulsarClientException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .subscribe();
        consumer.receive();
        return consumer;
    }

//    @Bean
    Consumer pulsarExclusiveConsumerB() throws PulsarClientException {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(commonConfig.getTestTopic())
                .consumerName("consumerExclusiveB")
                .subscriptionName("subscription-Exclusive")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionMode(SubscriptionMode.Durable)
                .messageListener(new MessageListener<String>() {
                    @Override
                    public void received(Consumer<String> consumer, Message<String> msg) {
                        logger.info("received msg, topic: {}, value: {} ", msg.getTopicName(), msg.getValue());
                    }
                })
                .subscribe();
        return consumer;
    }

    @Bean
    Consumer pulsarExclusiveConsumerC() throws PulsarClientException {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(commonConfig.getTestTopic())
                .consumerName("consumerExclusiveC")
                .subscriptionName("subscription-Exclusive")
                .subscriptionType(SubscriptionType.Exclusive)
                .messageListener(new MessageListener<String>() {
                    @Override
                    public void received(Consumer<String> consumer, Message<String> msg) {
                        try {
                            logger.info("{} received msg, topic: {}, value: {} ", consumer.getConsumerName(), msg.getTopicName(), msg.getValue());
                            consumer.acknowledge(msg);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .subscribe();
        return consumer;
    }


}

