package com.ascentstream.demo;

import com.ascentstream.demo.pulsar.listener.PulsarListener4Exclusive;
import com.ascentstream.demo.pulsar.listener.PulsarListener4Failover;
import com.ascentstream.demo.pulsar.listener.PulsarListener4KeyShared;
import com.ascentstream.demo.pulsar.listener.PulsarListener4Shared;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.pulsar.PulsarAutoConfiguration;
import org.springframework.boot.autoconfigure.pulsar.PulsarProperties;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.pulsar.annotation.EnablePulsar;

/**
 * Hello world!
 *
 */
@AutoConfigurationPackage(
        basePackageClasses = {PulsarAutoConfiguration.class, PulsarProperties.class}
)
@SpringBootConfiguration
@EnableAutoConfiguration
@ComponentScan(basePackages = {"com.ascentstream.demo"},
        excludeFilters = {
        @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = {
                PulsarListener4Exclusive.class,
                PulsarListener4Failover.class,
                PulsarListener4Shared.class,
                PulsarListener4KeyShared.class,
        })
})
@EnablePulsar
public class PulsarSpringbootDemoApplication extends SpringBootServletInitializer {

    private static final Logger logger = LoggerFactory.getLogger(PulsarSpringbootDemoApplication.class);

    public static void main( String[] args ) {
        ConfigurableApplicationContext ac = SpringApplication.run(PulsarSpringbootDemoApplication.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        return builder.sources(PulsarSpringbootDemoApplication.class);
    }

}
