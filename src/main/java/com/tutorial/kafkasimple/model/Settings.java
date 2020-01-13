package com.tutorial.kafkasimple.model;

import java.util.HashMap;
import java.util.Map;

public class Settings {
    private Map<String, String> settings;
    private boolean isReady;

    public Settings() {
        settings = new HashMap<>();
        isReady = false;
    }

    public boolean isReady() {
        return isReady;
    }

    public void setReady(boolean ready) {
        isReady = ready;
    }


    public Map<String, String> getSettings() {
        return settings;
    }

    public void addSetting(String key, String value) {
        settings.put(key, value);
    }

    public void removeSetting(String key) {
        settings.remove(key);
    }

    public void setSettings(Map<String, String> settings) {
        this.settings = settings;
    }


}
