package it.polimi.middleware.kafka_pipeline;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.threads.TaskManager;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;
import java.util.Collections;

public class MainTaskManager {

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("Usage: java -jar <path_to_jar> <id> <threads_num> <config_file_path>");
            System.exit(-1);
        }

        int id = Integer.parseInt(args[0]);
        int threads_num = Integer.parseInt(args[1]);
        String config_path = args[2];

        Config.CONFIG_FILE = config_path;

        // Parse global configurations
        new Parser();
        Parser.parseConfig();

        Config.printConfiguration();

        new TaskManager(id, threads_num).start();

    }
}
