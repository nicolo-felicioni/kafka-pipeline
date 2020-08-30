package it.polimi.middleware.kafka_pipeline;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.threads.JobManager;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;

import java.util.*;

public class MainJobManager {

    public static void main(String[] args) {

        // Parse global configurations
        new Parser();
        Parser.parseConfig();

        Config.printConfiguration();

        TopicsManager topicsManager = TopicsManager.getInstance();
        // create topics
        List<String> topics = Parser.parseTopics();
        topicsManager.setSourceTopic(Config.SOURCE_TOPIC);
        topicsManager.setSinkTopic(Config.SINK_TOPIC);
        topicsManager.createTopics(topics);
        topicsManager.createTopics(Collections.singletonList(Config.HEARTBEAT_TOPIC));
        topicsManager.createTopics(Collections.singletonList(Config.SETTING_THREADS_TOPIC));
        topicsManager.createTopics(Collections.singletonList(Config.SETTINGS_TOPIC));

        /*
            TODO : jobmanager should ask task managers how many
                   threads they can handle (avoid strugglers)
         */
        new JobManager().start();
    }
}
