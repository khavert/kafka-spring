package com.tutorial.kafkasimple.services;

import com.tutorial.kafkasimple.configuration.KafkaConfiguration;
import com.tutorial.kafkasimple.configuration.KafkaConsumerConfiguration;
import com.tutorial.kafkasimple.model.Event;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Service
public class KafkaConsumerService {
    private List<Event> events;
    private KafkaConsumerConfiguration consumerConfiguration;
    private KafkaConfiguration configuration;
    private ConcurrentMessageListenerContainer<String, String> container;

    KafkaConsumerService(KafkaConfiguration configuration, KafkaConsumerConfiguration consumerConfiguration) {
        events = new ArrayList<>();
        this.consumerConfiguration = consumerConfiguration;
        this.configuration = configuration;
    }

    public List<Event> getEvents() {
        return events;
    }

    public MessageListener<String, String> messageListener() {
        MessageListener<String, String> messageListener = record -> {
            events.add(new Event(record.key(), record.value(), record.partition(), record.offset()));
        };
        return messageListener;
    }

    @Bean
    public void start() {
        ContainerProperties containerProperties = new ContainerProperties(configuration.getTopic());
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        containerProperties.setKafkaConsumerProperties(props);
        containerProperties.setMessageListener(messageListener());
        container = new ConcurrentMessageListenerContainer<>(consumerConfiguration.consumerFactory(), containerProperties);
        container.start();
    }

    public void stop() {
        if (container.isRunning()) {
            container.stop();
        }
    }

    public String rewindOffset(long offset) {
        container.stop();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrap());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, configuration.getGroup_id());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(configuration.getTopic(), 0);

        kafkaConsumer.assign(Collections.singleton(topicPartition));
        System.out.println("Current position " + kafkaConsumer.position(topicPartition));
        kafkaConsumer.seek(topicPartition, offset);
        kafkaConsumer.commitSync();
        System.out.println("Current position " + kafkaConsumer.position(topicPartition));
        kafkaConsumer.close();
        container.start();

        return "Offset was set to " + offset;
    }

}
