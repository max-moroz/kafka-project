package com.kafka.calculatortest.service;

import com.fox.generator.Number;
import com.fox.multiplier.SquaringKafkaMessage;
import com.kafka.calculatortest.CalculatorTestApplication;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@SpringBootTest(classes = CalculatorTestApplication.class)
public class KafkaMessageServiceTest extends AbstractTestNGSpringContextTests {

    private static final int PARTITION_INDEX = 0;

    @Autowired
    @Qualifier("sum")
    private KafkaConsumer kafkaSumConsumer;

    @Autowired
    @Qualifier("multiplier")
    private KafkaConsumer kafkaMultiplierConsumer;

    @Value("${kafka.sum.topic.name}")
    private String kafkaSumTopicName;

    @Value("${kafka.square.topic.name}")
    private String kafkaSquareTopicName;

    private int firstNumber;
    private int secondNumber;

    private long sum;


    @BeforeClass
    void initEnvironment() throws UnirestException, JSONException {
        HttpResponse<JsonNode> request = Unirest.post("http://127.0.0.1:8090/generate")
                .header("accept", "application/json")
                .header("content-type", "application/json")
                .asJson();

        JSONObject object = request.getBody().getObject();

        this.firstNumber = object.getInt("numberOne");
        this.secondNumber = object.getInt("numberTwo");

        System.out.println("FirstNumber= " + firstNumber + "SecondNumber= " + secondNumber);
        this.sum = firstNumber + secondNumber;
    }


    @Test(priority = 1)
    void adder_shouldConsumeTwoIntegers_whenGeneratorSentThem() {
        int firstInteger;
        int secondInteger;

        List<TopicPartition> topicPartition = Collections.singletonList(new TopicPartition(kafkaSumTopicName, PARTITION_INDEX));
        kafkaSumConsumer.assign(topicPartition);
        kafkaSumConsumer.seekToEnd(topicPartition);
        long currentPosition = kafkaSumConsumer.position(topicPartition.get(PARTITION_INDEX)) - 1;  // the latest message
        kafkaSumConsumer.seek(topicPartition.get(PARTITION_INDEX), currentPosition);

        ConsumerRecords<String, Number> records = kafkaSumConsumer.poll(Duration.ofMillis(100));

        firstInteger = records.iterator().next().value().getNumberOne();
        secondInteger = records.iterator().next().value().getNumberTwo();

        assertTrue(firstNumber == firstInteger && secondNumber == secondInteger);
    }


    @Test(priority = 2)
    void adder_shouldProduceSumOfTwoIntegers_whenConsumedTheseInts() {
        long kafkaSumMessage;
        List<TopicPartition> topicPartition = Collections.singletonList(new TopicPartition(kafkaSumTopicName, PARTITION_INDEX));
        kafkaSumConsumer.assign(topicPartition);
        kafkaSumConsumer.seekToEnd(topicPartition);
        long currentPosition = kafkaSumConsumer.position(topicPartition.get(PARTITION_INDEX)) - 1;  // the latest message
        kafkaSumConsumer.seek(topicPartition.get(PARTITION_INDEX), currentPosition);
        ConsumerRecords<String, Number> records = kafkaSumConsumer.poll(Duration.ofMillis(1000));

        kafkaSumMessage = records.iterator().next().value().getNumberOne()
                + records.iterator().next().value().getNumberTwo();

        assertEquals(sum, kafkaSumMessage);
    }


    @Test(priority = 3)
    void multiplier_shouldSquareSum_whenConsumedMessageFromAdder() {
        String actualValue = null; // actual kafka square topic message value
        BigInteger expectedValue = BigInteger.valueOf(sum * sum);

        List<TopicPartition> topicPartition = Collections.singletonList(new TopicPartition(kafkaSquareTopicName, PARTITION_INDEX));
        kafkaMultiplierConsumer.assign(topicPartition);
        kafkaMultiplierConsumer.seekToEnd(topicPartition);
        long currentPosition = kafkaMultiplierConsumer.position(topicPartition.get(PARTITION_INDEX)) - 1;
        kafkaMultiplierConsumer.seek(topicPartition.get(PARTITION_INDEX), currentPosition);

        ConsumerRecords<String, SquaringKafkaMessage> records = kafkaMultiplierConsumer.poll(Duration.ofMillis(1000));
        actualValue = records.iterator().next().value().getSquare();

        assertEquals(expectedValue.toString(), actualValue);
    }
}
