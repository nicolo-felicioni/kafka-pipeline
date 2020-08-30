package it.polimi.middleware.kafka_pipeline;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.threads.TaskManager;

public class MainTaskManager1 {

    public static void main(String[] args) {

        // Parse global configurations
        new Parser();
        Parser.parseConfig();

        Config.printConfiguration();

        /*TopicsManager topicsManager = TopicsManager.getInstance();
        topicsManager.createTopics(Collections.singletonList(Config.HEARTBEAT_TOPIC));
        topicsManager.createTopics(Collections.singletonList(Config.SETTINGS_TOPIC));*/

        TaskManager taskManager = new TaskManager(1, 2);
        taskManager.createThreads();
        taskManager.waitStartSettings();
        taskManager.sendThreadsNumber();
        taskManager.waitSerializedPipeline();
        taskManager.start();

    }
}
