package it.polimi.middleware.kafka_pipeline;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.pipeline.Pipeline;
import it.polimi.middleware.kafka_pipeline.pipeline.Task;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.threads.ThreadsExecutor;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;

import java.util.*;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        Parser parser = new Parser();
        TopicsManager topicsManager = TopicsManager.getInstance();

        // Parse global configurations
        Config config = parser.parseConfig();
        Config.printConfiguration();

        // Define properties for consumers and producers
        Properties producerProps = Utils.getProducerProperties();
        Properties consumerProps = Utils.getConsumerProperties();

        // pipeline containing all the nodes (stream processors)
        Pipeline pipeline = parser.parsePipeline(producerProps, consumerProps);

        short topicsNumPartitions = 1;
        short topicsReplicationFactor = 1;

        // create source and sink topics
        List<String> globalTopics = parser.parseSourceSinkTopics();

        topicsManager.setSourceTopic(globalTopics.get(0));
        topicsManager.setSinkTopic(globalTopics.get(1));

        topicsManager.createPipelineTopics(pipeline.getProcessorsMap(), topicsNumPartitions, topicsReplicationFactor);

        // create a list of tasks
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < Config.TASKS_NUM; i++) {
            Task task = new Task(i, pipeline.clone());
            tasks.add(task);
        }

        // assign tasks to the executor that will spawn threads
        ThreadsExecutor executor = new ThreadsExecutor(tasks);

        //Thread.sleep(5000);

        //executor.shutdown();
    }
}
