package it.polimi.middleware.kafka_pipeline.parser;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.ProcessorType;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessorProperties;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import org.yaml.snakeyaml.Yaml;

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
        Config.JOB_NAME = String.valueOf(yaml_config.get(2).get("job_name"));
        Config.PROCESSORS_CONSUMER_GROUP = "processors_" + Config.JOB_NAME;
        Config.TM_NUMBER = yaml_config.get(3).get("tm_number");
        Config.PARALLELISM = yaml_config.get(4).get("parallelism");
        //Config.NUM_TOPICS_PARTITIONS = yaml_config.get(5).get("num_topics_partitions");
        Config.REPLICATION_FACTOR = yaml_config.get(5).get("replication_factor").shortValue();
        Config.SOURCE_TOPIC = String.valueOf(yaml_config.get(6).get("source_topic"));
        Config.SINK_TOPIC = String.valueOf(yaml_config.get(7).get("sink_topic"));
        return config;
    }

    /**
     * @return a list containing topics names.
     *         The first 2 elements are the global topics, that are the
     *         input and the output topics from the pipeline point of view
     */
    public static List<String> parseTopics() {

        ArrayList<String> topics = new ArrayList<>();

        topics.add(Config.SOURCE_TOPIC);
        topics.add(Config.SINK_TOPIC);

        try (InputStream in = (Parser.class).getClassLoader().getResourceAsStream(Config.PIPELINE_FILE)) {
            Iterable<Object> itr = yaml.loadAll(in);
            for (Object o : itr) {
                List<LinkedHashMap> list = (ArrayList<LinkedHashMap>) o;
                for (LinkedHashMap hm : list) {
                    String processorID = (String)hm.get("id");
                    List<String> to = (List<String>)hm.get("to");

                    // add output topics
                    for (String t : to) {
                        //System.out.println(t);
                        String outTopic = TopicsManager.getOutputTopic(processorID, t);
                        if (!topics.contains(outTopic)) {
                            topics.add(outTopic);
                        }
                    }

                    // add state topic
                    String stateTopic = TopicsManager.getStateTopic(processorID);
                    if (!topics.contains(stateTopic)) {
                        topics.add(stateTopic);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*ArrayList<Map<String, String>> yaml_objs = parseYaml(Config.PIPELINE_FILE);

        System.out.println(yaml_objs);

        ArrayList<String> topics = new ArrayList<>();

        topics.add(Config.SOURCE_TOPIC);
        topics.add(Config.SINK_TOPIC);

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
        }*/

        //System.out.println(topics);

        return topics;
    }

    /**
     * @param pipelineID
     * @return a map containing all the stream processors and their IDs
     */
    public static List<StreamProcessorProperties> parsePipeline(int pipelineID) {

        Map<String, StreamProcessorProperties> propertiesMap = new HashMap<>();
        StreamProcessorProperties properties;

        try (InputStream in = (Parser.class).getClassLoader().getResourceAsStream(Config.PIPELINE_FILE)) {
            Iterable<Object> itr = yaml.loadAll(in);
            for (Object o : itr) {
                //System.out.println("Loaded object type: " + o.getClass());
                List<LinkedHashMap> list = (ArrayList<LinkedHashMap>) o;
                //System.out.println("-- the list --");
                //System.out.println(list);
                //System.out.println("-- iterating --");

                for (LinkedHashMap hm : list) {
                    String processorID = (String)hm.get("id");
                    ProcessorType processorType = Utils.getProcessorType((String)hm.get("type"));
                    List<String> to = (List<String>)hm.get("to");

                    if (!propertiesMap.containsKey(processorID)) {
                        properties = new StreamProcessorProperties(
                                pipelineID,
                                processorID,
                                processorType);
                        for (String t : to) {
                            properties.addOutput(t);
                        }

                        System.out.println(properties);

                        propertiesMap.put(processorID, properties);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
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
                System.out.println(properties.getID());
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
