package com.arav.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    public static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {
        sendData();
    }

    private static void sendData() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9001");
        properties.setProperty("client.id",ProducerDemoKeys.class.getName());
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "100");
        // Create a producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        String topic = "demo_java";
        for(int batch=0;batch<2;batch++){
            for (int i = 0; i < 3000; i++) {
                String key = "id_"+i;
                String value = "hello "+i;
                // Create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key,value);
                // Send data
                kafkaProducer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Key: %s | Partition: %d".formatted(key,metadata.partition()));
                    } else {
                        log.error(exception.getLocalizedMessage());
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        kafkaProducer.flush();
        //flush and close the producer
        kafkaProducer.close();
    }

}
