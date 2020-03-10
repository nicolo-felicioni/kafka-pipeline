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
        System.out.println("Tasks num: " + Config.TASKS_NUM + " - Parallelism: " + Config.PARALLELISM + "\n");

        // Parse pipeline structure and nodes
        ArrayList<Map<String, String>> yaml_objs = parser.parseYaml(Config.PIPELINE_FILE);
        //System.out.println(yaml_objs);

        // Map containing all the nodes (keys are IDs)
        Map<String, StreamProcessor> processorsMap = new HashMap<>();

        // Define properties for consumers and producers
        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:"+Config.SERVER_PORT);
        producerProps.put("group.id", "group");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("key.deserializer", StringDeserializer.class.getName());
        producerProps.put("value.deserializer", StringDeserializer.class.getName());
        //producerProps.put("transactional.id", "transaction_id");
        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:"+Config.SERVER_PORT);
        consumerProps.put("group.id", "group");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        //consumerProps.put("isolation.level", "idempotent");
        //consumerProps.put("enable.auto.commit", "false");

        // create all the StreamProcessors
        StreamProcessor processor = null;
        for (Map<String, String> obj: yaml_objs) {
            if (obj.get("type").equals("source")) {
                processor = new MockSource(obj.get("id"), obj.get("type"), obj.get("from"), obj.get("to"), producerProps, consumerProps);
            }
            else if (obj.get("type").equals("sink")) {
                processor = new MockSink(obj.get("id"), obj.get("type"), obj.get("from"), obj.get("to"), producerProps, consumerProps);
            }
            processorsMap.put(processor.getId(), processor);
            System.out.println(processor);
        }

        TopicsManager topicsManager = new TopicsManager();
        TopicsManager.createTopics(Arrays.asList(new NewTopic("test_topic", (short)1, (short)1)));

        // Create pipeline and tasks
        Pipeline pipeline = new Pipeline(processorsMap);
        int topicsNumPartitions = 1;
        short topicsReplicationFactor = 1;
        pipeline.buildPipelineTopics(topicsNumPartitions, topicsReplicationFactor);

        // create a list of tasks
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < Config.TASKS_NUM; i++) {
            Task task = new Task(i, pipeline.clone());
            tasks.add(task);
        }

        // assign tasks to the executor that will spawn threads
        ThreadsExecutor executor = new ThreadsExecutor(tasks);

        Thread.sleep(5000);

        executor.shutdown();
    }
}
