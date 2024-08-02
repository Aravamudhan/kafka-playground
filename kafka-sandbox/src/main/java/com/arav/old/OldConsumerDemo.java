package com.arav.old;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class OldConsumerDemo {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "consumer_demoapp_2";
    private static final String TOPIC = "test_topic";

    public static void main(String[] args) {
        getData();
    }

    private static void getData() {
        System.out.println("Starting the kafka consumer......");
        // Consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", GROUP_ID);
        properties.setProperty("auto.offset.reset", "earliest");

        // Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic
        consumer.subscribe(Arrays.asList(TOPIC));

        // poll for data
        while (true) {
            System.out.println("Polling for the  message...");
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("Key:%s,Value:%s,Partition:%d,Offset:%d", record.key(), record.value(), record.partition(), record.offset()));
            }
        }
    }
}
