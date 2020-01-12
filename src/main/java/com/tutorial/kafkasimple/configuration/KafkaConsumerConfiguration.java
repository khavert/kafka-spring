package com.tutorial.kafkasimple.configuration;

import com.tutorial.kafkasimple.services.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;

import java.util.*;
import java.util.stream.Collectors;

@EnableKafka
@Configuration
public class KafkaConsumerConfiguration {
    private KafkaConfiguration configuration;
    KafkaConsumerService consumerService;

    KafkaConsumerConfiguration(KafkaConfiguration configuration, KafkaConsumerService consumerService){
        this.configuration = configuration;
        this.consumerService = consumerService;
    }


    public ConsumerFactory<String,String> consumerFactory(){
        Map<String,Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,configuration.getBootstrap());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,configuration.getGroup_id());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        return new DefaultKafkaConsumerFactory<>(props);
    }


    public ConcurrentKafkaListenerContainerFactory<String,String> kafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String,String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public void start(){
        rewindOffset();
        ContainerProperties containerProperties = new ContainerProperties("events");
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        containerProperties.setKafkaConsumerProperties(props);
        containerProperties.setMessageListener(consumerService.messageListener());
        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory(),containerProperties);
        container.start();
    }


    private void rewindOffset(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,configuration.getBootstrap());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,configuration.getGroup_id());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        TopicPartition topicPartition = new TopicPartition(configuration.getTopic(),0);

        kafkaConsumer.assign(Collections.singleton(topicPartition));
        System.out.println("Current position " + kafkaConsumer.position(topicPartition));
        kafkaConsumer.seek(topicPartition,0);
        kafkaConsumer.commitSync();
        System.out.println("Current position " + kafkaConsumer.position(topicPartition));
        kafkaConsumer.close();
    }
}
