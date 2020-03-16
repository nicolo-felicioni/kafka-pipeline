package it.polimi.middleware.kafka_pipeline.topics;

import it.polimi.middleware.kafka_pipeline.pipeline.Pipeline;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicsManager {

    private static AdminClient admin;

    private static TopicsManager topicsManagerInstance = null;

    /**
     * Private constructor.
     * It is private since the class is a singleton.
     */
    private TopicsManager() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        // add something to properties
        admin = KafkaAdminClient.create(props);
    }


    /**
     * TopicsManager getter. The TopicsManager is a singleton.
     * @return an instance of the TopicsManager.
     */
    public static TopicsManager getInstance(){
        if (topicsManagerInstance == null){
            //thread safe creation
            synchronized (TopicsManager.class){
                if (topicsManagerInstance == null) {
                    topicsManagerInstance = new TopicsManager();
                }
            }
        }
        return topicsManagerInstance;
    }

    //TODO: why is this function here?
    public static String getTopicName(String s1, String s2) {
        return s1 + "_" + s2;
    }

    public void createTopics(List<String> newTopics, short numPartitions, short replicationFactor) {
        for(String topic : newTopics) {
            try {
                CreateTopicsResult result = admin.createTopics(Collections.singletonList(new NewTopic(topic, numPartitions, replicationFactor)));
                System.out.println("Created topic " + topic);
            } catch (TopicExistsException e) {
                // do nothing
            }
        }
        System.out.println("Created topics");
    }
}
