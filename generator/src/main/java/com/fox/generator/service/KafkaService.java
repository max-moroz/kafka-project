package com.fox.generator.service;

import com.fox.generator.Number;
import com.fox.generator.entity.NumberPojo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import java.util.concurrent.ThreadLocalRandom;

@Controller
public class KafkaService {

    @Autowired
    private KafkaProducer<String, Number> kafkaProducer;

    @Value("${kafka.topic.name}")
    private String topicName;

    public NumberPojo generateDigits() {
        int minRange = -50000;
        int maxRange = 50000;
        Number randomDigits = Number.newBuilder()
                .setNumberOne(ThreadLocalRandom.current().nextInt(minRange, maxRange + 1))
                .setNumberTwo(ThreadLocalRandom.current().nextInt(minRange, maxRange + 1))
                .build();
        sendMessage(randomDigits);
        return NumberPojo.getFromAvro(randomDigits);
    }

    private void sendMessage(Number randomDigits) {
        ProducerRecord<String, Number> producerRecord = new ProducerRecord<>(topicName, randomDigits);
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                System.out.println("Sent message= [ " + randomDigits + " ] with offset= [" + recordMetadata.offset() + "]");

            } else {
                System.out.println("Unable to send message=[ " + randomDigits + " ] due to: " + e.getMessage());
            }
        });
    }
}
