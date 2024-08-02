package com.arav.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9001";
    private static final String GROUP_ID = "consumer_demoapp_1";
    private static final String TOPIC = "demo_java";
//    private static final String TOPIC = "test_topic";
    public static void main(String[] args) {
        getData();
    }

    private static void getData(){
        log.info("Starting the kafka consumer......");
        // Consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",BOOTSTRAP_SERVERS);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("group.id",GROUP_ID);
        properties.setProperty("auto.offset.reset","earliest");

        // Create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic
        consumer.subscribe(Arrays.asList(TOPIC));

        // poll for data
        while (true){
            log.info(".");
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(5000));
            for(ConsumerRecord<String,String> record:records){
                log.info("Key:{},Value:{},Partition:{},Offset:{}",record.key(),record.value(),record.partition(),record.offset());
            }
        }
    }

}
