package com.ascentstream.demo.config;

import com.ascentstream.demo.PulsarProducerService;
import com.ascentstream.demo.entity.CommonStatic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarConsumeConfigShared {

    private final CommonConfig commonConfig;
    private final PulsarClient pulsarClient;
    private final PulsarProducerService pulsarProducerService;

    private static final Logger logger = LoggerFactory.getLogger(PulsarConsumeConfigShared.class);

    public PulsarConsumeConfigShared(CommonConfig commonConfig, PulsarClient pulsarClient,
                                     PulsarProducerService pulsarProducerService) {
        this.commonConfig = commonConfig;
        this.pulsarClient = pulsarClient;
        this.pulsarProducerService = pulsarProducerService;
    }


    @Bean
    Consumer pulsarSharedConsumerA() throws PulsarClientException {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(commonConfig.getTestTopic())
                .consumerName("consumerSharedA")
                .subscriptionName("subscription-Shared")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
//                .negativeAckRedeliveryDelay(30, TimeUnit.SECONDS)
//                .enableRetry(true)
//                .deadLetterPolicy(DeadLetterPolicy.builder()
//                        .maxRedeliverCount(50)
//                        .build())
                .messageListener(new MessageListener<String>() {
                    @Override
                    public void received(Consumer<String> consumer, Message<String> msg) {
                        logger.info("{} received msg, value: {}, messageID: {} ", consumer.getConsumerName(), msg.getValue(), msg.getMessageId().toString());
                        try {
                            CommonStatic.recordReceivedMessage(msg.getValue());
                            consumer.acknowledge(msg);
//                            pulsarProducerService.sendMessage4Delay(msg.getValue(), 30);
                        } catch (PulsarClientException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .subscribe();
        return consumer;
    }

    @Bean
    Consumer pulsarSharedConsumerB() throws PulsarClientException {
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(commonConfig.getTestTopic())
                .consumerName("consumerSharedB")
                .subscriptionName("subscription-Shared")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
//                .negativeAckRedeliveryDelay(30, TimeUnit.SECONDS)
//                .enableRetry(true)
//                .deadLetterPolicy(DeadLetterPolicy.builder()
//                        .maxRedeliverCount(50)
//                        .build())
                .messageListener(new MessageListener<String>() {
                    @Override
                    public void received(Consumer<String> consumer, Message<String> msg) {
                        logger.info("{} received msg, value: {}, messageID: {} ", consumer.getConsumerName(), msg.getValue(), msg.getMessageId().toString());
                        try {
                            CommonStatic.recordReceivedMessage(msg.getValue());
                            consumer.acknowledge(msg);
//                            pulsarProducerService.sendMessage4Delay(msg.getValue(), 30);
                        } catch (PulsarClientException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .subscribe();
        return consumer;
    }


}

