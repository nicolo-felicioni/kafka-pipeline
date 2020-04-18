package it.polimi.middleware.kafka_pipeline;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.threads.JobManager;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;

import java.util.*;

public class Main {

    public static void main(String[] args) {


        new Parser();

        // Parse global configurations
        Config config = Parser.parseConfig();

        Config.printConfiguration();

        TopicsManager topicsManager = TopicsManager.getInstance();

        // create topics
        List<String> topics = Parser.parseTopics();
        topicsManager.setSourceTopic(topics.get(0));
        topicsManager.setSinkTopic(topics.get(1));
        topicsManager.createTopics(topics);

        // create tasks
        /*List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < Config.TASKS_NUM; i++) {
            Task task = new Task(i);
            tasks.add(task);
        }*/


        /*
            TODO : jobmanager should ask task  managers how many
                   threads they can handle (avoid strugglers)
         */
        JobManager jobManager = new JobManager();

        jobManager.start();

    }
}
