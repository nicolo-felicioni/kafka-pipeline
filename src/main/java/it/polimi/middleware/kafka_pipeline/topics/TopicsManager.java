package it.polimi.middleware.kafka_pipeline.topics;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import java.util.*;

public class TopicsManager {

    private static AdminClient admin;

    private static final String SOURCE_KEYWORD = "source";
    private static final String SINK_KEYWORD = "sink";
    private static String SOURCE_TOPIC = "source_topic";
    private static String SINK_TOPIC = "sink_topic";


    private static TopicsManager topicsManagerInstance = null;

    /**
     * Private constructor.
     * It is private since the class is a singleton.
     */
    private TopicsManager() {
        Properties props = new Properties();
        System.out.println(Config.SERVER_IP+":"+Config.SERVER_PORT);
        props.put("bootstrap.servers", Config.SERVER_IP+":"+Config.SERVER_PORT);
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

    private static String getTopicName(String from, String to) {
        return from + "_" + to;
    }

    public static String getInputTopic(String from, String id) {
        if(from.equals(SOURCE_KEYWORD))
            return SOURCE_TOPIC;
        else
            return getTopicName(from, id);
    }

    public static String getStateTopic(String id) {
        return getTopicName("state_topic", id);
    }

    public static String getOutputTopic(String id, String to) {
        if(to.equals(SINK_KEYWORD))
            return SINK_TOPIC;
        else
            return TopicsManager.getTopicName(id, to);
    }

    public boolean isSource(String processorName){
        //TODO maybe this method is useless
        return false;
    }

    public void setSourceTopic(String sourceTopic) {
        SOURCE_TOPIC = sourceTopic;
    }

    public void setSinkTopic(String sinkTopic) {
        SINK_TOPIC = sinkTopic;
    }

    /**
     * Create topics starting from a list of strings.
     * @param newTopics         the list of topics to be created
     */
    public void createTopics(List<String> newTopics) {
        List<String> createdTopics = new ArrayList<>();
        for(String topic : newTopics) {
            try {
                if (!createdTopics.contains(topic)) {
                    CreateTopicsResult result = admin.createTopics(Collections.singletonList(
                            new NewTopic(topic, Config.NUM_TOPICS_PARTITIONS, Config.REPLICATION_FACTOR))
                    );
                    System.out.println("Created topic " + topic);
                    createdTopics.add(topic);
                }
            } catch (TopicExistsException e) {
                // do nothing
            }
        }
        System.out.println("Created topics");
    }

    /**
     * Create topics starting from the description of the pipeline.
     * @param processorsMap     map containing all the stream processors (the pipeline)
     */
    public void createPipelineTopics(Map<String, StreamProcessor> processorsMap) {
        List<String> topics = new ArrayList<>();
        for(String id : processorsMap.keySet()) {
            topics.add(processorsMap.get(id).getInputTopic());
            topics.add(processorsMap.get(id).getOutputTopic());
        }
        createTopics(topics);
    }

    //TODO temporary, for debug
    public String getSourceTopic(){
        return SOURCE_TOPIC;
    }
}
