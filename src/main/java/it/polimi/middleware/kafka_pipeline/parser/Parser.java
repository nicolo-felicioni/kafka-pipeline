package it.polimi.middleware.kafka_pipeline.parser;

import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.ArrayList;

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
}
