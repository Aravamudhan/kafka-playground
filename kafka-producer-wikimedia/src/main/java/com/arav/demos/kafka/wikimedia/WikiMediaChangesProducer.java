package com.arav.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikiMediaChangesProducer {
    private static final Logger log = LoggerFactory.getLogger(WikiMediaChangesProducer.class);
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9001";
    private static final String TOPIC_NAME = "wikimedia.recentchange";
    private static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) {
        sendData();
    }

    private static void sendData() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",BOOTSTRAP_SERVERS);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty("batch.size","100");
        // Safe producer settings. These are set by default in the kafka>=3.0 version
        properties.setProperty("acks","all");// if acks is all then min.insync.replicas should be atleast 2
        properties.setProperty("enable.idempotence","true");
        properties.setProperty("retries",String.valueOf(Integer.MAX_VALUE));
        properties.setProperty("max.in.flight.requests.per.connection","5");
        // High throughput producer at the cost of cpu cycles
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);
        // Event handler
        EventHandler eventHandler = new WikiMediaChangeHandler(kafkaProducer,TOPIC_NAME);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(eventHandler, URI.create(WIKIMEDIA_URL));
        EventSource eventSource = eventSourceBuilder.build();
        eventSource.start();
        try {
            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
