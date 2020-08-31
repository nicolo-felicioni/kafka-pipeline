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

        new JobManager().start();
    }
}
