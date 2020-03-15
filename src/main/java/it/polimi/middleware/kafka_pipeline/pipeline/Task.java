package it.polimi.middleware.kafka_pipeline.pipeline;

import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Task {

    private int id;
    private Pipeline pipeline;

    public Task(int id, Pipeline pipeline) {
        this.id = id;
        this.pipeline = pipeline;
    }

    public void proceed() {
        pipeline.process();
    }

    public void stop() {
        pipeline.stopPipeline();
    }

    public int getId() { return this.id; }

    public void createTopics(short numPartitions, short replicationFactor) {
        List<String> topics = new ArrayList<>();
        Map<String, StreamProcessor> processorsMap = pipeline.getProcessorsMap();  // get a copy of the processor map
        for(String id : processorsMap.keySet()) {
            topics.add(processorsMap.get(id).getInputTopic() + "_" + this.id);  // distinguish topics in each
            topics.add(processorsMap.get(id).getOutputTopic() + "_" + this.id); // task by adding the task id
        }
        TopicsManager.createTopics(topics, numPartitions, replicationFactor);
    }

}
