package com.ascentstream.demo;

import com.ascentstream.demo.config.CommonConfig;
import java.util.Random;
import com.ascentstream.demo.entity.CommonStatic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;


@Component
public class PulsarDemoApplicationRunner implements ApplicationRunner {


    private static Logger logger = LoggerFactory.getLogger(PulsarDemoApplicationRunner.class);

    private final CommonConfig commonConfig;
    private final PulsarClient  pulsarClient;
    private final PulsarProducerService pulsarProducerService;

    private static Long datetime = 0L;


    Producer<String> producer = null;

    public PulsarDemoApplicationRunner(CommonConfig commonConfig, PulsarClient pulsarClient,
                                       PulsarProducerService pulsarProducerService) {
        this.commonConfig = commonConfig;
        this.pulsarClient = pulsarClient;
        this.pulsarProducerService = pulsarProducerService;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("PulsarDemoApplication Started.");
//        verifyDelayedMessageMissed();
        sendMessages();
    }


    private void sendMessages() throws InterruptedException {
        int count = 0;
        while (true) {
            String message = String.format("Message %d", count++);
            pulsarProducerService.sendMessage(message);
            logger.info("Message sent: {}", message);
            Thread.sleep(10*1000);
        }
    }

    private void verifyDelayedMessageMissed() throws PulsarClientException {

        datetime += System.currentTimeMillis() + 1000L*60*5;

        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    logger.info("map4VerifyingDelayedMsg size: {}", CommonStatic.map4VerifyingDelayedMsg.size());
                    Thread.sleep(10*1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();


        int count = 0, delay = 30;
        Random random = new Random(20);
        while (true) {
            if (count > 9 || System.currentTimeMillis() > datetime) {
                logger.info("Stop produce message, sent size: {}, map4VerifyingDelayedMsg size: {}", count, CommonStatic.map4VerifyingDelayedMsg.size());
                break;
            }
            // message-${count}
            String message = String.format("message-%d", count);
            pulsarProducerService.sendMessage4Delay(message, delay);
            CommonStatic.recordSentMessage(message);
            count++;
        }



    }

}
