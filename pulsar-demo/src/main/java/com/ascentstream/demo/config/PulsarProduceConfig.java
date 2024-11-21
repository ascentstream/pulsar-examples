package com.ascentstream.demo.config;

import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarProduceConfig {

    private final CommonConfig commonConfig;
    private final PulsarClient pulsarClient;

    public PulsarProduceConfig(CommonConfig commonConfig, PulsarClient pulsarClient) {
        this.commonConfig = commonConfig;
        this.pulsarClient = pulsarClient;
    }


    @Bean
    Producer pulsarProducer() throws PulsarClientException {
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(commonConfig.getTestTopic())
//                .batcherBuilder(BatcherBuilder.KEY_BASED)
                .create();
        return producer;
    }


}
