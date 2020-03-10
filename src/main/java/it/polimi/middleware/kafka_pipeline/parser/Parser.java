package it.polimi.middleware.kafka_pipeline.parser;

import it.polimi.middleware.kafka_pipeline.common.Config;
import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;

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
        Config.TASKS_NUM = yaml_config.get(1).get("tasks_num");
        Config.PARALLELISM = yaml_config.get(2).get("parallelism");
    }
}
