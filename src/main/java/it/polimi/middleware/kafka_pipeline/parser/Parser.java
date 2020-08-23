package it.polimi.middleware.kafka_pipeline.parser;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessorProperties;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;
import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.*;

/**
 * Parser for Yaml configuration files
 */
public class Parser {

    private static Yaml yaml;

    public Parser() {
        yaml = new Yaml();
    }

    /**
     * @return a Config object containing application configurations
     */
    public static Config parseConfig() {
        Config config = new Config();
        ArrayList<Map<String, Integer>> yaml_config = parseYaml(Config.CONFIG_FILE);
        Config.SERVER_IP = String.valueOf(yaml_config.get(0).get("server_ip"));
        Config.SERVER_PORT = yaml_config.get(1).get("server_port");
        Config.GROUP = String.valueOf(yaml_config.get(2).get("group"));
        Config.PARALLELISM = yaml_config.get(3).get("parallelism");
        Config.REPLICATION_FACTOR = yaml_config.get(4).get("replication_factor").shortValue();
        Config.NUM_TOPICS_PARTITIONS = yaml_config.get(5).get("num_topics_partitions");
        return config;
    }

    /**
     * @return a list containing topics names.
     *         The first 2 elements are the global topics, that are the
     *         input and the output topics from the pipeline point of view
     */
    public static List<String> parseTopics() {
        ArrayList<Map<String, String>> yaml_objs = parseYaml(Config.PIPELINE_FILE);

        System.out.println(yaml_objs);

        ArrayList<String> topics = new ArrayList<>();

        String sourceTopic = yaml_objs.get(0).get("source_topic");
        String sinkTopic = yaml_objs.get(1).get("sink_topic");
        topics.add(sourceTopic);
        topics.add(sinkTopic);

        for (int i = 2; i < yaml_objs.size(); i++) {
            Map<String, String> obj = yaml_objs.get(i);
            //String inTopic = TopicsManager.getInputTopic(obj.get("id"), obj.get("from"));
            String outTopic = TopicsManager.getOutputTopic(obj.get("id"), obj.get("to"));
            String stateTopic = TopicsManager.getStateTopic(obj.get("id"));
            //if (!topics.contains(inTopic)) {
            //    topics.add(inTopic);
            //}
            if (!topics.contains(outTopic)) {
                topics.add(outTopic);
            }
            if (!topics.contains(stateTopic)) {
                topics.add(stateTopic);
            }
        }

        return topics;
    }

    /**
     * @param pipelineID
     * @return a map containing all the stream processors and their IDs
     */
    public static List<StreamProcessorProperties> parsePipeline(int pipelineID) {
        // Parse pipeline structure and nodes
        ArrayList<Map<String, String>> yaml_objs = parseYaml(Config.PIPELINE_FILE);

        System.out.println("Parsed yaml: " + yaml_objs);

        Map<String, StreamProcessorProperties> propertiesMap = new HashMap<>();
        StreamProcessorProperties properties;

        for (int i = 2; i < yaml_objs.size(); i++) {
            Map<String, String> obj = yaml_objs.get(i);

            String processorID = obj.get("id");

            if (!propertiesMap.containsKey(processorID)) {
                properties = new StreamProcessorProperties(pipelineID, processorID, Utils.getProcessorType(obj.get("type")));
                propertiesMap.put(processorID, properties);
            }

            properties = propertiesMap.get(processorID);
            //properties.addInput(obj.get("from"));
            properties.addOutput(obj.get("to"));
        }

        // for each processor, get the "to" field and assign
        // the same IDs as input to the destination node
        for (String processorID : propertiesMap.keySet()) {
            properties = propertiesMap.get(processorID); // node's properties
            for (String destinationProcessor : properties.getTo()) {
                // destinationProcessor : properties of the node with id equal to the one in the "to" field
                if (!destinationProcessor.equals("sink")) {
                    StreamProcessorProperties destinationProperties = propertiesMap.get(destinationProcessor);
                    //System.out.println(processorID);
                    destinationProperties.addInput(processorID);
                }
            }
        }

        // the node having no input arcs will is set as source
        for (String processorID : propertiesMap.keySet()) {
            properties = propertiesMap.get(processorID);
            if (properties.getFrom().size() == 0) {
                properties.addInput("source");
            }
        }

        System.out.println("Properties map: " + propertiesMap);

        return new ArrayList<>(propertiesMap.values());
    }

    /**
     * @param filename path to the file to be parsed
     * @return ArrayList containing a Map<String,String> for each parsed object
     */
    private static ArrayList parseYaml(String filename) {
        InputStream inputStream = (Parser.class)
                .getClassLoader()
                .getResourceAsStream(filename);
        return yaml.load(inputStream);
    }
}
