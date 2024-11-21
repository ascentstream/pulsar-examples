package com.ascentstream.demo.pulsar.config;


import com.ascentstream.demo.config.CommonConfig;
import com.ascentstream.demo.entity.Employee;
import com.ascentstream.demo.entity.RequestBody;
import com.ascentstream.demo.entity.User;
import org.apache.pulsar.client.api.Schema;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.SchemaResolver;

@Configuration
public class PulsarBaseConfig {

    private final CommonConfig commonConfig;

    public PulsarBaseConfig(CommonConfig commonConfig) {
        this.commonConfig = commonConfig;
    }

    @Bean
    public SchemaResolver.SchemaResolverCustomizer<DefaultSchemaResolver> schemaResolverCustomizer() {
        return (schemaResolver) -> {
            schemaResolver.addCustomSchemaMapping(RequestBody.class, Schema.JSON(RequestBody.class));
            schemaResolver.addCustomSchemaMapping(User.class, Schema.JSON(User.class));
            schemaResolver.addCustomSchemaMapping(Employee.class, Schema.JSON(Employee.class));
        };
    }

}
