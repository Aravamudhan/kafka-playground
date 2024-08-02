package com.arav.demos.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class AdminHelper {

    private AdminClient adminClient = null;
    private final String brokerUrl;

    public AdminHelper(String brokerUrl){
        this.brokerUrl = brokerUrl;
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,brokerUrl);
        properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,"100");
        properties.setProperty("client.id","com.arav.demos.kafka.admin.AdminHelper");
        this.adminClient = AdminClient.create(properties);
    }
    public static void main(String[] args) {
        AdminHelper adminHelper = new AdminHelper("localhost:9092");
//        adminHelper.displayTopics();
//        adminHelper.displayProducers();
        adminHelper.displayAllProducers("demo_java");
//        adminHelper.describeTopics();
//        adminHelper.createTopic("demo_topic_by_admin");
//        try {
//            // Wait for sometime before calling the delete method
//            Thread.sleep(3000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        adminHelper.deleteTopic("demo_topic_by_admin");
//        adminHelper.displayBrokerConfiguration();
        adminHelper.shutdown();
    }
    public void shutdown(){
        if(adminClient!=null){
            adminClient.close();
        }
    }
    public void displayTopics(){
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        listTopicsResult.names().whenComplete((topics, throwable) -> {
            if(throwable!=null){
                System.out.println("Error in fetching the topics");
                throwable.printStackTrace();
            }else{
                System.out.println("Topics are:");
                topics.forEach(System.out::println);
            }
        });
    }
    public Set<String> getTopics(){
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        try{
            Set<String> topicNames = listTopicsResult.names().get();
            return topicNames;
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return Collections.emptySet();
    }
    public void describeTopics(){
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(getTopics());
        try{
            Map<String, KafkaFuture<TopicDescription>> topicDescriptionMap = describeTopicsResult.topicNameValues();
            topicDescriptionMap.forEach((topicName,topicDescription)->{
                try {
                    System.out.println(String.format("Topic name:[%s], Topic Description:[%s]",topicName,topicDescription.get().toString()));
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public boolean createTopic(String topicName){
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(getTopics());
        try{
            Map<String, KafkaFuture<TopicDescription>> topicDescriptionMap = describeTopicsResult.topicNameValues();
            for(Map.Entry<String,KafkaFuture<TopicDescription>> entry:topicDescriptionMap.entrySet()){
               if(entry.equals(topicName)){
                   System.out.println(String.format("Topic [%s] already exists",topicName));
                   return false;
               }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // If not create
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(new NewTopic(topicName,3,(short)1)));
        try {
            createTopicsResult.all().get();
            System.out.println(String.format("Topic [%s] has been created successfully",topicName));
            return true;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Some problem in creating the topic [%s]",topicName));
        return false;
    }
    public boolean deleteTopic(String topicName){
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(getTopics());
        try{
            Map<String, KafkaFuture<TopicDescription>> topicDescriptionMap = describeTopicsResult.topicNameValues();
            if(!topicDescriptionMap.containsKey(topicName)){
                System.out.println(String.format("Topic [%s] does not exist",topicName));
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // If not create
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
        try {
            deleteTopicsResult.all().get();
            System.out.println(String.format("Topic [%s] has been deleted successfully",topicName));
            return true;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Some problem in deleting the topic [%s]",topicName));
        return false;
    }

    public void displayBrokerConfiguration(){
        KafkaFuture<Map<ConfigResource,Config>> kafkaFutureMap = adminClient.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC,"wikimedia.recentchange"))).all();
        try{
            Map<ConfigResource,Config> configResult = kafkaFutureMap.get(2000, TimeUnit.MILLISECONDS);
            configResult.forEach((configResource,config)->{
                System.out.println(String.format("Config Resource:[%s], Config:[%s]",configResource,config));
            });
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void displayProducers(){
        TopicPartition topicPartition1 = new TopicPartition("demo_java",0);
        TopicPartition topicPartition2 = new TopicPartition("demo_java",1);
        TopicPartition topicPartition3 = new TopicPartition("demo_java",2);

        DescribeProducersResult producersResult = adminClient.describeProducers(Arrays.asList(topicPartition1,topicPartition2,topicPartition3));
        KafkaFuture<Map<TopicPartition, DescribeProducersResult.PartitionProducerState>> result = producersResult.all();
        try {
            Map<TopicPartition, DescribeProducersResult.PartitionProducerState> resultMap = result.get();
            resultMap.entrySet().stream().forEach(entry->{
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void displayAllProducers(String topicName) {
        Map<String, List<TopicPartition>> partitionsMap = new HashMap<>();
        try {
            adminClient.describeTopics(Collections.singletonList(topicName)).all().get()
                    .forEach((topic, topicDescription) -> {
                        List<TopicPartition> partitions = new ArrayList<>();
                        topicDescription.partitions().forEach(partitionInfo ->
                                partitions.add(new TopicPartition(topicName, partitionInfo.partition())));
                        partitionsMap.put(topicName, partitions);
                    });
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        for (TopicPartition partition : partitionsMap.get(topicName)) {
            DescribeProducersResult result = adminClient.describeProducers(Collections.singletonList(partition));
            try {
                result.all().get().get(partition).activeProducers().stream().forEach(producerState -> {
                    System.out.println("Producer : " + producerState);
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
