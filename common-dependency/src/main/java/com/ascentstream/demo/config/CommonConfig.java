package com.ascentstream.demo.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommonConfig {

    @Value("${pulsar.topic.test-topic: test-topic}")
    private String testTopic;

    @Value("${pulsar.topic.test-topic-p: test-topic-p}")
    private String testTopicP;

    @Value("${pulsar.topic.test-topic-json: test-topic-json}")
    private String testTopicJson;

    @Value("${pulsar.topic.test-topic-json-p: test-topic-json-p}")
    private String testTopicJsonP;

    @Value("${pulsar.client.service-pulsar-url}")
    private String servicePulsarUrl;

    @Value("${pulsar.client.service-http-url}")
    private String serviceHttpUrl;

    public String getTestTopic() {
        return testTopic;
    }

    public void setTestTopic(String testTopic) {
        this.testTopic = testTopic;
    }

    public String getTestTopicP() {
        return testTopicP;
    }

    public void setTestTopicP(String testTopicP) {
        this.testTopicP = testTopicP;
    }

    public String getTestTopicJson() {
        return testTopicJson;
    }

    public void setTestTopicJson(String testTopicJson) {
        this.testTopicJson = testTopicJson;
    }

    public String getTestTopicJsonP() {
        return testTopicJsonP;
    }

    public void setTestTopicJsonP(String testTopicJsonP) {
        this.testTopicJsonP = testTopicJsonP;
    }

    public String getServicePulsarUrl() {
        return servicePulsarUrl;
    }

    public void setServicePulsarUrl(String servicePulsarUrl) {
        this.servicePulsarUrl = servicePulsarUrl;
    }

    public String getServiceHttpUrl() {
        return serviceHttpUrl;
    }

    public void setServiceHttpUrl(String serviceHttpUrl) {
        this.serviceHttpUrl = serviceHttpUrl;
    }
}
