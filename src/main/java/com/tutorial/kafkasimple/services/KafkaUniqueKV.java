package com.tutorial.kafkasimple.services;

import com.tutorial.kafkasimple.configuration.KafkaConfiguration;
import com.tutorial.kafkasimple.configuration.KafkaConsumerConfiguration;
import com.tutorial.kafkasimple.model.Settings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Properties;

@Service
public class KafkaUniqueKV {

    private KafkaTemplate<String, String> template;
    private KafkaConsumerConfiguration consumerConfiguration;
    private KafkaConfiguration config;
    private final Settings settings = new Settings();

    public KafkaUniqueKV(KafkaTemplate<String, String> template, KafkaConsumerConfiguration consumerConfiguration, KafkaConfiguration config) {
        this.template = template;
        this.consumerConfiguration = consumerConfiguration;
        this.config = config;
    }

    @Bean
    public void startUnique() {
        ContainerProperties containerProperties = new ContainerProperties(config.getSettings_topic());
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        containerProperties.setKafkaConsumerProperties(props);
        containerProperties.setMessageListener(messageUniqueListener());
        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerConfiguration.consumerFactory(), containerProperties);
        container.start();
    }

    private MessageListener<String, String> messageUniqueListener() {
        return record -> {
            if (record.value() == null) {
                settings.removeSetting(record.key());
            } else {
                settings.addSetting(record.key(), record.value());
            }
        };
    }

    public void updateSetting(String key, String value) {
        template.send(config.getSettings_topic(), key, value);
    }

    public Map<String, String> getSettings() {
        return settings.getSettings();
    }
}
