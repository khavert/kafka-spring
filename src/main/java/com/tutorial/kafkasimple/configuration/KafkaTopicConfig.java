package com.tutorial.kafkasimple.configuration;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {
    private KafkaConfiguration configuration;

    KafkaTopicConfig(KafkaConfiguration configuration) {
        this.configuration = configuration;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrap());
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,"1000");
        return new KafkaAdmin(config);
    }

    @Bean
    public NewTopic createTopic(){
        return new NewTopic(configuration.getTopic(),1,(short)1);
    }

    @Bean
    public NewTopic createCompactedTopic(){
        NewTopic newTopic = new NewTopic(configuration.getSettings_topic(),1,(short)1);
        Map<String,String> config = new HashMap<>();
        config.put("cleanup.policy","compact");
        newTopic.configs(config);
        return newTopic;
    }
}
