package com.ascentstream.demo.pulsar.service;

import com.ascentstream.demo.config.CommonConfig;
import com.ascentstream.demo.entity.Employee;
import com.ascentstream.demo.entity.RequestBody;
import com.ascentstream.demo.pulsar.config.PulsarProducer;
import com.ascentstream.demo.util.DateUtil;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.stereotype.Service;

@Service
public class PulsarProducerService {

    private final static String TEST_MESSAGE = "This is a test message";
    private final PulsarProducer pulsarProducer;
    private final CommonConfig commonConfig;

    public PulsarProducerService(PulsarProducer pulsarProducer, CommonConfig commonConfig) {
        this.pulsarProducer = pulsarProducer;
        this.commonConfig = commonConfig;
    }


    public MessageId sendTestMessage() throws PulsarClientException {
        // No topic assigned, send message to test topic.
        return sendMessage(TEST_MESSAGE + ", " + DateUtil.getDateTimeNow());
    }

    public MessageId sendTestMessage4Json() throws PulsarClientException {
        // No topic assigned, send message to test topic.
        RequestBody body = new RequestBody();
        Employee employee = new Employee("001", "Xuwei", 33);
        employee.setTitle("Software Engineer");
        body.setFrom("test");
        body.setUser(employee);
        return sendMessage(commonConfig.getTestTopicJson(), body);
    }

    public <T> MessageId sendMessage(T message) throws PulsarClientException {
        // No topic assigned, send message to test topic.
        return sendMessage(commonConfig.getTestTopic(), message);
    }

    public <T> MessageId sendMessage(String topic, T message) throws PulsarClientException {
        return pulsarProducer.sendMessage(topic, message);
    }

    public <T> MessageId sendMessage(String topic, T message, CompressionType compressionType) throws PulsarClientException {
        return pulsarProducer.sendMessage(topic, message, compressionType);
    }

    public <T> MessageId sendMessageWithKey(String key) throws PulsarClientException {
        return pulsarProducer.sendMessageWithKey(commonConfig.getTestTopic(), TEST_MESSAGE + ", " + DateUtil.getDateTimeNow(), key);
    }

    public <T> MessageId sendMessageWithKey(T message, String key) throws PulsarClientException {
        return pulsarProducer.sendMessageWithKey(commonConfig.getTestTopic(), message, key);
    }

    public <T> MessageId sendMessageWithKey(String topic, T message, String key) throws PulsarClientException {
        return pulsarProducer.sendMessageWithKey(topic, message, key);
    }

    public <T> MessageId sendDelayedMessage(long delay) throws PulsarClientException {
        return sendDelayedMessage(TEST_MESSAGE + ", " + DateUtil.getDateTimeNow(), delay);
    }

    public <T> MessageId sendDelayedMessage(T message, long delay) throws PulsarClientException {
        return sendDelayedMessage(commonConfig.getTestTopic(), message, delay);
    }

    public <T> MessageId sendDelayedMessage(String topic, T message, long delay) throws PulsarClientException {
        return pulsarProducer.sendDelayedMessage(topic, message, delay);
    }
}
