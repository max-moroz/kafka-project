package com.kafka.calculatortest.config;

import com.fox.generator.Number;
import com.fox.multiplier.SquaringKafkaMessage;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${kafka.boot.server}")
    private String kafkaServer;

    @Value("${kafka.sum.consumer.group.id}")
    private String kafkaSumGroupId;

    @Value("${kafka.multiplier.consumer.group.id}")
    private String kafkaMultiplierGroupId;

    @Bean(name = "sum")
    public KafkaConsumer<String, Number> sumConsumer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaSumGroupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("auto.commit.enable", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("specific.avro.reader", "true");
        return new KafkaConsumer<>(config);
    }

    @Bean(name = "multiplier")
    public KafkaConsumer<String, SquaringKafkaMessage> multiplierConsumer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
//        config.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaMultiplierGroupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("auto.commit.enable", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("specific.avro.reader", "true");
        return new KafkaConsumer<>(config);
    }
}
