package com.ascentstream.demo.pulsar.config;

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.PulsarListenerConsumerBuilderCustomizer;

@Configuration
public class PulsarListenerCustomizerConfig {

    @Bean
    PulsarListenerConsumerBuilderCustomizer<String> consumerRetryCustomizer() {
        return (builder) -> builder
                .receiverQueueSize(500)
                .enableRetry(true)
                .deadLetterPolicy(deadLetterPolicy())
                ;
    }


    @Bean
    PulsarListenerConsumerBuilderCustomizer<String> consumerDeadCustomizer() {
        return (builder) -> builder
                .receiverQueueSize(500)
                .deadLetterPolicy(deadLetterPolicy())
                ;
    }

    @Bean
    PulsarListenerConsumerBuilderCustomizer<String> consumerBatchReceiveCustomizer() {
        return (builder) -> builder
                .receiverQueueSize(500)
                .batchReceivePolicy(batchReceivePolicy())
                ;
    }

    @Bean
    BatchReceivePolicy batchReceivePolicy() {
        return BatchReceivePolicy.builder()
                .maxNumBytes(1024*1024*10)
                .maxNumMessages(5)
                .timeout(1000*10, TimeUnit.MILLISECONDS).
                build();
    }

    @Bean
    DeadLetterPolicy deadLetterPolicy() {
        return DeadLetterPolicy.builder()
                .deadLetterTopic("test-topic-dead")
                .maxRedeliverCount(1)
                .build()
                ;
    }

    @Bean
    PulsarListenerConsumerBuilderCustomizer<String> consumerCommonCustomizer() {
        return (builder) -> builder
                .receiverQueueSize(500)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                ;
    }
}
