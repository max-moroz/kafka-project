package com.fox.multiplier.service;

import com.fox.generator.Number;
import com.fox.multiplier.SquaringKafkaMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Controller;

@Controller
public class KafkaService {

    @Autowired
    private KafkaProducer<String, SquaringKafkaMessage> kafkaProducer;

    @Value("${kafka.topic.name}")
    private String topicName;

    @Value("${kafka.producer.topic.name}")
    private String producerTopicName;

    @KafkaListener(topics = "${kafka.topic.name}")
    void listenToPartitionWithOffset(Number number) {
        if (number.getSum() != null) {
            long sum = number.getSum();
            System.out.println("Message consumed = " + sum);
            sendMessage(sum);
        }
    }

    private SquaringKafkaMessage squareSum(long sum) {
        String square = String.valueOf(sum * sum);
        return SquaringKafkaMessage.newBuilder()
                .setSum(sum)
                .setSquare(square)
                .build();
    }

    private void sendMessage(long sum) {
        SquaringKafkaMessage square = squareSum(sum);
        ProducerRecord<String, SquaringKafkaMessage> producer = new ProducerRecord<>(producerTopicName, "square", square);
        kafkaProducer.send(producer, (recordMetadata, e) -> {
            if (e == null) {
                System.out.println("Sent message= [ " + square + " ] with offset= [" + recordMetadata.offset() + "]");

            } else {
                System.out.println("Unable to send message=[ " + square + " ] due to: " + e.getMessage());
            }
        });
    }
}
