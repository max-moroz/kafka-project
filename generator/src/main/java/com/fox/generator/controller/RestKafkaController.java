package com.fox.generator.controller;

import com.fox.generator.entity.NumberPojo;
import com.fox.generator.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RestKafkaController {

    @Autowired
    KafkaService controller;

    @PostMapping(value = "/generate")
    public NumberPojo generatePostTwoDigitsKafka(){
        return controller.generateDigits();
    }
}
