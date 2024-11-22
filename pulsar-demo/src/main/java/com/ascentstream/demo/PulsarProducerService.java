package com.ascentstream.demo;


import com.ascentstream.demo.config.CommonConfig;
import com.ascentstream.demo.entity.Employee;
import com.ascentstream.demo.entity.RequestBody;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import com.ascentstream.demo.util.JSONUtil;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PulsarProducerService {

    private static final Logger logger = LoggerFactory.getLogger(PulsarProducerService.class);

    private final PulsarClient pulsarClient;
    private final CommonConfig commonConfig;
    private final Producer  pulsarProducer;

    public PulsarProducerService(PulsarClient pulsarClient, CommonConfig commonConfig, Producer pulsarProducer) {
        this.pulsarClient = pulsarClient;
        this.commonConfig = commonConfig;
        this.pulsarProducer = pulsarProducer;
    }

    public void sendMessage4Delay(String message, long delay) throws PulsarClientException {
        pulsarProducer.newMessage()
                .deliverAfter(delay, TimeUnit.SECONDS)
                .value(message)
                .send();

    }

    public void sendMessage(String message) {
        sendMessage(commonConfig.getTestTopic(), message);
    }


    public void sendMessage(String topic, String message) {
        Producer<String> producer = null;
        try {
            producer = pulsarClient.newProducer(Schema.STRING)
                    .topic(topic)
                    .batcherBuilder(BatcherBuilder.KEY_BASED)
                    .create();
            producer.newMessage()
                    .value(message)
                    .send();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        } finally {
            if (producer != null) {
                try {
                    producer.close();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void sendTestJsonMsg() throws PulsarClientException {
        Producer<RequestBody> producer = pulsarClient.newProducer(Schema.JSON(RequestBody.class))
                .topic(commonConfig.getTestTopicJson())
                .batcherBuilder(BatcherBuilder.KEY_BASED)
                .create();

        RequestBody body = new RequestBody();
        Employee employee = new Employee("002", "Xuwei", 33);
        employee.setTitle("Software Engineer");
        body.setFrom("test");
        body.setUser(employee);

        CompletableFuture<MessageId> completableFuture = producer.sendAsync(body);
        completableFuture.whenComplete(((messageId, throwable) -> {
            if( null != throwable ) {
                logger.error("sendAsyncMessage failed: {}", throwable.getMessage(), throwable);
            } else {
                try {
                    logger.info("sendAsyncMessage success: messageId={}, value={}", messageId, JSONUtil.convertToString(body));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }));
    }
}
