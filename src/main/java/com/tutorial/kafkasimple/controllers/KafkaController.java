package com.tutorial.kafkasimple.controllers;

import com.tutorial.kafkasimple.configuration.KafkaConfiguration;
import com.tutorial.kafkasimple.model.Event;
import com.tutorial.kafkasimple.model.ResponseKafka;
import com.tutorial.kafkasimple.model.Setting;
import com.tutorial.kafkasimple.services.KafkaConsumerService;
import com.tutorial.kafkasimple.services.KafkaUniqueKV;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController(value = "simple")
public class KafkaController {

    @Autowired
    private KafkaTemplate<String,String> template;

    @Autowired
    private KafkaConsumerService consumerService;

    @Autowired
    private KafkaConfiguration config;

    @Autowired
    private KafkaUniqueKV uniqueKV;

    @PostMapping(path = "/send")
    public ResponseEntity<ResponseKafka> sendMessage(@RequestBody Event message) throws ExecutionException, InterruptedException {
        RecordMetadata recordMetadata = template.send(config.getTopic(),message.getKey(), message.getValue()).get().getRecordMetadata();
        ResponseKafka responseKafka = new ResponseKafka(
                recordMetadata.topic(),recordMetadata.partition(),recordMetadata.offset(),recordMetadata.timestamp()
        );
        return ResponseEntity.ok(responseKafka);
    }

    @GetMapping(path = "/received")
    public List<Event> events(){
        return consumerService.getEvents();
    }

    @PostMapping(path = "/offset/{offset}")
    @ResponseBody
    public String resetOffset(@PathVariable long offset){
        return consumerService.rewindOffset(offset);
    }

    @GetMapping(path = "/settings")
    public Map<String,String> getSettings(){
        return uniqueKV.getSettings();
    }

    @PostMapping(path = "/settings")
    public String setSetting(@RequestBody Setting setting){
        uniqueKV.updateSetting(setting.key,setting.value);
        return "Update settings with " + setting.key + "=" + setting.value;
    }
}
