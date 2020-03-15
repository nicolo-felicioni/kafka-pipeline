package it.polimi.middleware.kafka_pipeline.topics;

import it.polimi.middleware.kafka_pipeline.pipeline.Pipeline;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicsManager {

    private static AdminClient admin;

    public TopicsManager() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        // add something to properties
        admin = KafkaAdminClient.create(props);
    }

    public static String getTopicName(String s1, String s2) {
        return s1 + "_" + s2;
    }

    public static void createTopics(List<String> newTopics, short numPartitions, short replicationFactor) {
        for(String topic : newTopics) {
            try {
                CreateTopicsResult result = admin.createTopics(Arrays.asList(new NewTopic(topic, numPartitions, replicationFactor)));
                System.out.println("Created topic " + topic);
            } catch (TopicExistsException e) {
                // do nothing
            }
        }
        System.out.println("Created topics");
    }
}
