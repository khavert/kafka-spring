package com.tutorial.kafkasimple.services;

import com.tutorial.kafkasimple.configuration.KafkaConfiguration;
import com.tutorial.kafkasimple.model.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import org.springframework.kafka.support.KafkaHeaders;

@Service
public class KafkaConsumerService {
    private List<Event> events;
    private KafkaConfiguration configuration;

    KafkaConsumerService(KafkaConfiguration configuration) {
        this.configuration = configuration;
        events = new ArrayList<Event>();
    }

//    @KafkaListener(topics = "${kafka.topic}")
//    public void listen(String key, String value, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, @Header(KafkaHeaders.OFFSET) long offset){
//        events.add(new Event(key,value,partition,offset));
//    }

    public List<Event> getEvents(){
        return events;
    }

    public MessageListener<String,String> messageListener(){
        MessageListener<String,String> messageListener = new MessageListener<String, String>() {
            @Override
            public void onMessage(ConsumerRecord<String, String> record) {
                System.out.println("Offset: " + record.offset());
                events.add(new Event(record.key(),record.value(), record.partition(),record.offset()));
            }
        };
        return messageListener;
    }
}
