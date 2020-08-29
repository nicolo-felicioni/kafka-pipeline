package it.polimi.middleware.kafka_pipeline.common;

import com.google.gson.Gson;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessorProperties;

public class JsonPropertiesSerializer {

    private Gson gson;

    public JsonPropertiesSerializer() {
        gson = new Gson();
    }

    public String serialize(StreamProcessorProperties props) {
        return gson.toJson(props);
    }

    public StreamProcessorProperties deserialize(String s) {
        return gson.fromJson(s, StreamProcessorProperties.class);
    }
}
