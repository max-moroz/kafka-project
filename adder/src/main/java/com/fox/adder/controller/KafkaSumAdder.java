package com.fox.adder.controller;

import com.fox.generator.Number;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Controller;

@Controller
public class KafkaSumAdder {

    @Autowired
    private KafkaProducer<String, Number> kafkaProducer;

    @Value("${kafka.topic.name}")
    private String topicName;


    @KafkaListener(topics = "${kafka.topic.name}")
    public void listenToSumTopic(Number number) {
        if (number.getSum() == null) {
            System.out.println("Message consumed:" + number.toString());

            sendMessage(createSumMessage(number));
        }
    }

    private Number createSumMessage(Number number) {
        long result = number.getNumberOne() + number.getNumberTwo();
        return Number.newBuilder(number)
                .setSum(result)
                .build();
    }

    private void sendMessage(Number number) {
        ProducerRecord<String, Number> producerRecord = new ProducerRecord<>(topicName, "sum", number);
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                System.out.println("Sent message= [ " + number + " ] with offset= [" + recordMetadata.offset() + "]");

            } else {
                System.out.println("Unable to send message=[ " + number + " ] due to: " + e.getMessage());
            }
        });
    }
}
