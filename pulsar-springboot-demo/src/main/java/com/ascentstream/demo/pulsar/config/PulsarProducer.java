package com.ascentstream.demo.pulsar.config;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class PulsarProducer {

    private static final Logger logger = LoggerFactory.getLogger(PulsarProducer.class);

    private final PulsarTemplate pulsarTemplate;

    public PulsarProducer(PulsarTemplate pulsarTemplate) {
        this.pulsarTemplate = pulsarTemplate;
    }

    public <T> MessageId sendMessageWithKey(String topic, T message, String key) throws PulsarClientException {
        if (StringUtils.isEmpty(key)) {
            return sendMessage(topic, message);
        } else {
            return pulsarTemplate.newMessage(message)
                    .withTopic(topic)
                    .withMessageCustomizer(messageBuilder -> {
                        messageBuilder.key(key);
                    }).send();
        }

    }

    public <T> MessageId sendMessage(String topic, T message) throws PulsarClientException {
        return pulsarTemplate.send(topic, message);
    }

    public <T> MessageId sendMessage(String topic, T message, CompressionType compressionType) throws PulsarClientException {
        if (compressionType == null || compressionType == CompressionType.NONE) {
            return sendMessage(topic, message);
        } else {
            return pulsarTemplate.newMessage(message)
                    .withTopic(topic)
                    .withProducerCustomizer(producerBuilder -> {
                        producerBuilder.compressionType(compressionType);
                    }).send();
        }
    }

    public <T> MessageId sendDelayedMessage(String topic, T message, long delay) throws PulsarClientException {
        return pulsarTemplate.newMessage(message)
                .withTopic(topic)
                .withMessageCustomizer(messageBuilder -> {
                    messageBuilder.deliverAfter(delay, TimeUnit.SECONDS);
                })
                .send();
    }

    public <T> void sendAsyncMessage(String topic, T message) throws PulsarClientException {
        CompletableFuture<MessageId> completableFuture = pulsarTemplate.sendAsync(topic, message);
        completableFuture.whenComplete(((messageId, throwable) -> {
            if( null != throwable ) {
                logger.error("sendAsyncMessage failed: {}", throwable.getMessage(), throwable);
                // Todo: error handling, to do sending retry.

            } else {
                logger.info("sendAsyncMessage success: messageId={}, value={}", messageId, message);
            }
        }));
    }

}
