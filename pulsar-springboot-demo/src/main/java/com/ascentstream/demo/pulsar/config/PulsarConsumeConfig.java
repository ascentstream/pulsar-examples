package com.ascentstream.demo.pulsar.config;

import com.ascentstream.demo.config.CommonConfig;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarConsumeConfig {

    private final CommonConfig commonConfig;
    private final PulsarClient pulsarClient;


    public PulsarConsumeConfig(CommonConfig commonConfig, PulsarClient pulsarClient) {
        this.commonConfig = commonConfig;
        this.pulsarClient = pulsarClient;
    }


}

