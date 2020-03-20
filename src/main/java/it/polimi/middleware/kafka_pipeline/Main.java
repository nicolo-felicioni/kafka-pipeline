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

        // Parse global configurations
        Config config = parser.parseConfig();
        System.out.println("\nServer ip: " + Config.SERVER_IP);
        System.out.println("Server port: " + Config.SERVER_PORT);
        System.out.println("Tasks num: " + Config.TASKS_NUM);
        System.out.println("Parallelism: " + Config.PARALLELISM);
        System.out.println("Group: " + Config.GROUP + "\n");

        // Define properties for consumers and producers
        Utils utils = new Utils();
        Properties producerProps = utils.getProducerProperties();
        Properties consumerProps = utils.getConsumerProperties();

        // pipeline containing all the nodes (stream processors)
        Pipeline pipeline = parser.parsePipeline(producerProps, consumerProps);

        short topicsNumPartitions = 1;
        short topicsReplicationFactor = 1;

        // create source and sink topics
        List<String> globalTopics = parser.parseSourceSinkTopics();
        TopicsManager.getInstance().setSourceTopic(globalTopics.get(0));
        TopicsManager.getInstance().setSinkTopic(globalTopics.get(1));

        TopicsManager.getInstance().createPipelineTopics(pipeline.getProcessorsMap(), topicsNumPartitions, topicsReplicationFactor);

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
