package com.tutorial.kafkasimple.model;

public class Event {
    private String key;
    private String value;
    private int partition;
    private long offset;

    public Event(){

    }

    public Event(String key,String value, int partition, long offset){
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
