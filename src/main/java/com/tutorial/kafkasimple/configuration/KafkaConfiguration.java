package com.tutorial.kafkasimple.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfiguration {
    private String bootstrap;
    private String topic;
    private String settings_topic;

    public String getGroup_id() {
        return group_id;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }

    private String group_id;

    public String getSettings_topic() {
        return settings_topic;
    }

    public void setSettings_topic(String settings_topic) {
        this.settings_topic = settings_topic;
    }

    public String getBootstrap() {
        return bootstrap;
    }

    public String getTopic() {
        return topic;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
