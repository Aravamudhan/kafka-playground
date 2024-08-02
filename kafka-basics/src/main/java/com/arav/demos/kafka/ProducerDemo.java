package com.arav.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("enable.idempotence","false");
        properties.setProperty("acks","all");
        // Create a producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);
        // Create a producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("test_topic","1","hello-world");
        // Send data
        kafkaProducer.send(producerRecord);
        // The 2 lines below are being provided considering that this is an async producer and jvm might even exit before sending the data.
        // In a production application, since a producer will keep on running we would rarely do the flush and the close operations
        // Send all the data and block until that is done
        kafkaProducer.flush();
        //flush and close the producer
        kafkaProducer.close();
    }

}
