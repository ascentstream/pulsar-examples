package com.ascentstream.demo;

import com.ascentstream.demo.config.PulsarConsumeConfigExclusive;
import com.ascentstream.demo.config.PulsarConsumeConfigFailover;
import com.ascentstream.demo.config.PulsarConsumeConfigKeyShared;
import com.ascentstream.demo.config.PulsarConsumeConfigShared;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.pulsar.PulsarAutoConfiguration;
import org.springframework.boot.autoconfigure.pulsar.PulsarProperties;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

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
        excludeFilters = {@ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = {
                PulsarConsumeConfigExclusive.class,
                PulsarConsumeConfigFailover.class,
                PulsarConsumeConfigKeyShared.class,
                PulsarConsumeConfigShared.class,
        }
        )}
)
public class PulsarDemoApplication extends SpringBootServletInitializer {

    private static final Logger logger = LoggerFactory.getLogger(PulsarDemoApplication.class);

    public static void main( String[] args ) {
        ConfigurableApplicationContext ac = SpringApplication.run(PulsarDemoApplication.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        return builder.sources(PulsarDemoApplication.class);
    }
}
