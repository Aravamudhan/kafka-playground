package com.arav.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaChangeHandler implements EventHandler {
    private static final Logger log = LoggerFactory.getLogger(WikiMediaChangeHandler.class);
    private final KafkaProducer<String,String> kafkaProducer;
    private final String topic;

    public WikiMediaChangeHandler(KafkaProducer<String, String> kafkaProducer,String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;

    }
    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info("Received message %s".formatted(messageEvent.getData()));
        kafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()),(metadata,exception)->{
            if(exception==null){
                log.info("Received new metadata Topic: %s Partition: %d Offset: %d Timestamp: %d".formatted(metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()));
            } else {
                log.error(exception.getLocalizedMessage());
            }
        });

    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        log.error(throwable.getLocalizedMessage()) ;

    }
}
