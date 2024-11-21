package com.ascentstream.demo.config;


import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PulsarBaseConfig {

    private final CommonConfig commonConfig;

    public PulsarBaseConfig(CommonConfig commonConfig) {
        this.commonConfig = commonConfig;
    }


    @Bean
    PulsarClient pulsarClient() throws PulsarClientException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(commonConfig.getServicePulsarUrl())
                .build();
        return  client;
    }



    @Bean
    PulsarAdmin pulsarAdmin() throws PulsarClientException {
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(commonConfig.getServiceHttpUrl())
                .build();
        return admin;
    }

}
