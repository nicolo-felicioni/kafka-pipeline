package it.polimi.middleware.kafka_pipeline.pipeline;

import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import java.util.*;

/**
 * This class represents the processing pipeline.
 *
 * It contains a map collecting all the StreamProcessors.
 */
public class Pipeline {

    private Map<String, StreamProcessor> processorsMap;

    public Pipeline(Map<String, StreamProcessor> processorsMap) {
        this.processorsMap = processorsMap;
    }

    public void process() {
        for(String processorId : processorsMap.keySet())
            processorsMap.get(processorId).process();
    }

    public void stopPipeline() {
        for(String processorId : processorsMap.keySet())
            processorsMap.get(processorId).stop();
    }

    public Map<String, StreamProcessor> getProcessorsMap() {
        return this.clone(processorsMap);
    }

    public Map<String, StreamProcessor> clone(Map<String, StreamProcessor> original) {
        return new HashMap<>(original);
    }

    @Override
    public Pipeline clone() {
        return new Pipeline(this.getProcessorsMap());
    }

}
