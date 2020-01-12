package com.tutorial.kafkasimple.services;

import com.tutorial.kafkasimple.configuration.KafkaConfiguration;
import com.tutorial.kafkasimple.model.Event;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class KafkaConsumerService {
    private List<Event> events;
    private KafkaConfiguration configuration;

    KafkaConsumerService(KafkaConfiguration configuration) {
        this.configuration = configuration;
        events = new ArrayList<Event>();
    }

    @KafkaListener(topics = "${kafka.topic}")
    public void listen(String key, String value){
        events.add(new Event(key,value));
    }

    public List<Event> getEvents(){
        return events;
    }
}
