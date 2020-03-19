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

    private static String SOURCE_KEYWORD = "begin";
    private static String SINK_KEYWORD = "end";
    private static String SOURCE_TOPIC = "source_topic";
    private static String SINK_TOPIC = "sink_topic";


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
    private static String getTopicName(String from, String to) {
        return from + "_" + to;
    }

    public static String getInputTopic(String from, String id) {
        if(from.equals(SOURCE_KEYWORD))
            return SOURCE_TOPIC;
        else
            return getTopicName(from, id);
    }

    public static String getOutputTopic(String id, String to) {
        if(to.equals(SINK_KEYWORD))
            return SINK_TOPIC;
        else
            return TopicsManager.getTopicName(id, to);
    }

    public boolean isSource(String processorName){
        //TODO
        return false;
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
