package com.fox.generator.config;

import com.fox.generator.Number;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Value("${kafka.boot.server}")
    private String kafkaServer;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    private static final String PRODUCER_CONFIG_SCHEMA_KEY = "schema.registry.url";

    @Bean
    public KafkaProducer<String, Number> kafkaProducer() {
        return new KafkaProducer<>(producerConfig());
    }

    @Bean
    public Properties producerConfig() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(PRODUCER_CONFIG_SCHEMA_KEY, schemaRegistryUrl);
        return properties;
    }
}
