package com.ascentstream.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Hello world!
 *
 */
@Component
public class PulsarSpringbootDemoApplicationRunner implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(PulsarSpringbootDemoApplicationRunner.class);

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("PulsarSpringbootDemoApplication Started.");
    }
}
