package com.arav.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoGroups.class);
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String GROUP_ID = "consumer_demoapp_1";
    private static final String TOPIC = "demo_java";
    public static void main(String[] args) {
        sendData();
    }

    private static void sendData(){
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

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            log.info("Detected a shutdown. Exit by calling the consumer.wakeup().....");
            // Calling the wakeup is the best way to interrupt the consumer.poll() method.
            // This will cause the poll method to throw a WakeupException
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Error in shutting down the application",e);
            }
        }));

        // Subscribe to the topic
        consumer.subscribe(Arrays.asList(TOPIC));

        // poll for data
        try{
            while (true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String,String> record:records){
                    log.info("Key:{},Value:{},Partition:{},Offset:{}",record.key(),record.value(),record.partition(),record.offset());
                }
            }
        } catch (WakeupException e){
            log.info("Received shutdown signal and the consumer has thrown wakeup exception...");
        } catch(Exception e) {
            log.error("Error in consuming the message",e);
        }finally {
            log.info("Graceful shutdown of the consumer....");
            consumer.close();
        }
    }

}
