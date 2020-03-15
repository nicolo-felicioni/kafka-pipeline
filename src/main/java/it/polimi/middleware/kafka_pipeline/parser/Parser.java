package it.polimi.middleware.kafka_pipeline.parser;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.pipeline.Pipeline;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.processors.sinks.MockSink;
import it.polimi.middleware.kafka_pipeline.processors.sources.MockSource;
import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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

    public void parseConfig() {
        ArrayList<Map<String, Integer>> yaml_config = parseYaml(Config.CONFIG_FILE);
        Config.SERVER_PORT = yaml_config.get(0).get("server_port");
        Config.GROUP = String.valueOf(yaml_config.get(1).get("group"));
        Config.TASKS_NUM = yaml_config.get(2).get("tasks_num");
        Config.PARALLELISM = yaml_config.get(3).get("parallelism");
    }

    public Pipeline parsePipeline(Properties producerProps, Properties consumerProps) {
        // Parse pipeline structure and nodes
        ArrayList<Map<String, String>> yaml_objs = parseYaml(Config.PIPELINE_FILE);
        Map<String, StreamProcessor> processorsMap = new HashMap<>();
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
        return new Pipeline(processorsMap);
    }
}
