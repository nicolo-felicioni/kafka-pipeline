package it.polimi.middleware.kafka_pipeline.parser;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.pipeline.Pipeline;
import it.polimi.middleware.kafka_pipeline.processors.Forwarder;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.*;

public class Parser {

    private Yaml yaml;

    public Parser() {
        yaml = new Yaml();
    }

    public ArrayList parseYaml(String filename) {
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream(filename);
        return yaml.load(inputStream);
    }

    public Config parseConfig() {
        Config config = new Config();
        ArrayList<Map<String, Integer>> yaml_config = parseYaml(Config.CONFIG_FILE);
        Config.SERVER_IP = String.valueOf(yaml_config.get(0).get("server_ip"));
        Config.SERVER_PORT = yaml_config.get(1).get("server_port");
        Config.GROUP = String.valueOf(yaml_config.get(2).get("group"));
        Config.TASKS_NUM = yaml_config.get(3).get("tasks_num");
        Config.PARALLELISM = yaml_config.get(4).get("parallelism");
        return config;
    }

    public List<String> parseSourceSinkTopics() {
        ArrayList<Map<String, String>> yaml_objs = parseYaml(Config.PIPELINE_FILE);
        String sourceTopic = yaml_objs.get(0).get("source_topic");
        String sinkTopic = yaml_objs.get(1).get("sink_topic");
        return new ArrayList<>(Arrays.asList(sourceTopic, sinkTopic));
    }

    public Pipeline parsePipeline(Properties producerProps, Properties consumerProps) {
        // Parse pipeline structure and nodes
        ArrayList<Map<String, String>> yaml_objs = parseYaml(Config.PIPELINE_FILE);

        Map<String, StreamProcessor> processorsMap = new HashMap<>();
        // create all the StreamProcessors
        StreamProcessor processor = null;
        for (int i = 2; i < yaml_objs.size(); i++) {
            Map<String, String> obj = yaml_objs.get(i);
            if (obj.get("type").equals("forward")) {
                processor = new Forwarder(obj.get("id"), obj.get("type"), obj.get("from"), obj.get("to"), producerProps, consumerProps);
            }
            processorsMap.put(processor.getId(), processor);
        }

        System.out.println("Processors map: " + processorsMap);
        return new Pipeline(processorsMap);
    }
}
