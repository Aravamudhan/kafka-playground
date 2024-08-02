package com.arav.old;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class OldProducerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
//        properties.setProperty("enable.idempotence","true");
        properties.setProperty("acks","all");
        // Create a producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);
        // Create a producer record
        ProducerRecord<String,String> producerRecord;
        for(int i=0;i<10;i++){
            producerRecord= new ProducerRecord<>("test_topic",String.valueOf(i+1),"hello-world"+(i+11));
            // Send data
            kafkaProducer.send(producerRecord);
        }
        // The 2 lines below are being provided considering that this is an async producer and jvm might even exit before sending the data.
        // In a production application, since a producer will keep on running we would rarely do the flush and the close operations
        // Send all the data and block until that is done
        kafkaProducer.flush();
        //flush and close the producer
        kafkaProducer.close();
    }
}
