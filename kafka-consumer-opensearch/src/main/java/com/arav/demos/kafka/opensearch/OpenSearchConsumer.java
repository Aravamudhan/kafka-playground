package com.arav.demos.kafka.opensearch;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OpenSearchConsumer {
    private static Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
    private static final String ES_CLUSTER_HOST = "http://localhost:9200";
    private static final String INDEX_NAME = "wikimedia";
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9001";
    private static final String GROUP_ID = "consumer_opensearch_demo";
    private static final String TOPIC_NAME = "wikimedia.recentchange";
    private static final Gson gson = new Gson();
    public static void main(String[] args) throws IOException {
        RestHighLevelClient restHighLevelClient = getOpenSearchClient();
        GetIndexRequest getIndexRequest = new GetIndexRequest(INDEX_NAME);
        IndicesClient indicesClient = restHighLevelClient.indices();
        boolean isIndexAvailable = indicesClient.exists(getIndexRequest,RequestOptions.DEFAULT);
        if(isIndexAvailable){
            log.info("Index {} is already available",INDEX_NAME);
        } else {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
            restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        }
        KafkaConsumer<String,String> consumer = getKafkaConsumer();
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
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
        try(consumer;restHighLevelClient;){
            while(true){
                // poll for data
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                log.info("Found {} records",records.count());
                BulkRequest bulkRequest = new BulkRequest();
                for(ConsumerRecord<String,String> record:records){
                    try {
                        log.info("Read the key:{},partition:{},offset:{}",record.key(),record.partition(),record.offset());
                        String id = extractId(record.value());
                        // create a request
                        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id(id);
                        indexRequest.source(record.value(), XContentType.JSON);
                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {
                        log.error("Exception in processing the records by the consumer",e);
                    }
                }
                if(bulkRequest.numberOfActions()>0){
                    // commit once the batch is processed
                    BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted {} records",bulkResponse.getItems().length);
                    consumer.commitSync();
                    log.info("Offsets have been committed for the inserted records!");
                } else {
                    log.info("No records to insert into the elasticsearch ");
                }
            }
        } catch (WakeupException e){
            log.info("Received shutdown signal and the consumer has thrown wakeup exception...");
        } catch (Exception e){
            log.error("Exception in the consumer",e);
        } finally {
            consumer.close();
            restHighLevelClient.close();
        }
    }

    private static KafkaConsumer<String,String> getKafkaConsumer() {
        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        // Create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        return consumer;
    }

    private static RestHighLevelClient getOpenSearchClient(){
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(ES_CLUSTER_HOST);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
        return restHighLevelClient;
    }

    private static String extractId(String payload){
        String id = JsonParser.parseString(payload).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
        return id;
    }

}
