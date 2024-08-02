package com.arav.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    public static void main(String[] args) {
        sendData();
    }

    private static void sendData() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("batch.size","100");
        // Create a producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);
        for(int batch=0;batch<30;batch++) {
            for(int i=0;i<30;i++) {
                // Create a producer record
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","hello-world"+batch+i);
                // Send data
                kafkaProducer.send(producerRecord,(metadata,exception)->{
                    if(exception==null){
                        log.info("Received new metadata %nTopic: %s %nPartition: %d %nOffset: %d %nTimestamp: %d".formatted(metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()));
                    } else {
                        log.error(exception.getLocalizedMessage());
                    }
                });
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }

        }
        kafkaProducer.flush();
        //flush and close the producer
        kafkaProducer.close();
    }

}
