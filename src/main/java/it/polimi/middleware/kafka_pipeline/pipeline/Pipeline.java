package it.polimi.middleware.kafka_pipeline.pipeline;

import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.threads.PipelineThread;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.protocol.types.Field;

import java.util.*;
import java.util.concurrent.*;

public class Pipeline {

    private Map<String, StreamProcessor> processorsMap;

    public Pipeline(Map<String, StreamProcessor> processorsMap) {
        this.processorsMap = processorsMap;
    }

    public void process() {
        for(String processorId : processorsMap.keySet())
            processorsMap.get(processorId).process();
    }

    public void buildPipelineTopics(int numPartitions, short replicationFactor) {
        List<NewTopic> topics = new ArrayList<>();
        for(String id : processorsMap.keySet()) {
            topics.add(new NewTopic(processorsMap.get(id).getInputTopic(), numPartitions, replicationFactor));
            topics.add(new NewTopic(processorsMap.get(id).getOutputTopic(), numPartitions, replicationFactor));
        }
        TopicsManager.createTopics(topics);
    }

    public void stopPipeline() {
        for(String processorId : processorsMap.keySet())
            processorsMap.get(processorId).stop();
    }

    @Override
    public Pipeline clone() {
        Map<String, StreamProcessor> newMap = new HashMap<>();
        for (String id : processorsMap.keySet()) {
            newMap.put(id, processorsMap.get(id).clone());
        }
        return new Pipeline(newMap);
    }

}
