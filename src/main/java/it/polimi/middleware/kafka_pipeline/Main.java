package it.polimi.middleware.kafka_pipeline;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.pipeline.Pipeline;
import it.polimi.middleware.kafka_pipeline.pipeline.Task;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.processors.sinks.MockSink;
import it.polimi.middleware.kafka_pipeline.processors.sources.MockSource;
import it.polimi.middleware.kafka_pipeline.threads.ThreadsExecutor;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        //String configFile = args[1];

        Parser parser = new Parser();
        Config config = new Config();

        // Parse global configurations
        parser.parseConfig();
        System.out.println("\nServer port: " + Config.SERVER_PORT);
        System.out.println("Tasks num: " + Config.TASKS_NUM);
        System.out.println("Parallelism: " + Config.PARALLELISM);
        System.out.println("Group: " + Config.GROUP + "\n");

        // Define properties for consumers and producers
        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:"+Config.SERVER_PORT);
        producerProps.put("group.id", Config.GROUP);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("key.deserializer", StringDeserializer.class.getName());
        producerProps.put("value.deserializer", StringDeserializer.class.getName());
        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:"+Config.SERVER_PORT);
        consumerProps.put("group.id", Config.GROUP);
        consumerProps.put("partition.assignment.strategy", RoundRobinAssignor.class.getName());
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());

        // Map containing all the nodes (keys are IDs)
        Pipeline pipeline = parser.parsePipeline(producerProps, consumerProps);

        TopicsManager topicsManager = new TopicsManager();

        short topicsNumPartitions = 2;
        short topicsReplicationFactor = 2;

        // create a list of tasks
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < Config.TASKS_NUM; i++) {
            Task task = new Task(i, pipeline.clone());
            tasks.add(task);
            task.createTopics(topicsNumPartitions, topicsReplicationFactor);
        }

        // assign tasks to the executor that will spawn threads
        ThreadsExecutor executor = new ThreadsExecutor(tasks);

        //Thread.sleep(5000);

        //executor.shutdown();
    }
}
