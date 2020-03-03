package it.polimi.middleware.kafka_pipeline;

import it.polimi.middleware.kafka_pipeline.pipeline.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.pipeline.Pipeline;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        //String configFile = args[1];

        Parser parser = new Parser();

        // Parse global configurations
        ArrayList<HashMap<String, Integer>> config = parser.parseYaml("config.yaml");
        int tasksNum = config.get(0).get("tasks_num");
        int parallelism = config.get(1).get("parallelism");
        System.out.println("Tasks num: " + tasksNum + " - Parallelism: " + parallelism + "\n");

        // Parse pipeline structure and nodes
        ArrayList<HashMap<String, String>> yaml_objs = parser.parseYaml("pipeline.yaml");
        //System.out.println(yaml_objs);

        // Map containing all the nodes (keys are IDs)
        HashMap<String, StreamProcessor> processorsMap = new HashMap<>();

        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        // Idempotence = exactly once semantics between producer and partition
        props.put("enable.idempotence", true);

        for (HashMap<String, String> obj: yaml_objs) {
            //StreamProcessor processor = new StreamProcessor(obj.get("id"), obj.get("from"), obj.get("to"), props);
            //processorsMap.put(processor.getId(), processor);
            //System.out.println(processor);
            System.out.println("Id: " + obj.get("id") + " - " + obj.get("from") + " - " + obj.get("to"));
        }

        //Pipeline pipeline = new Pipeline(tasksNum, parallelism);
        //pipeline.buildPipeline(processorsMap);

    }
}
