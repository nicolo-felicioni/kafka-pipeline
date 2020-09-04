package it.polimi.middleware.kafka_pipeline.processors;

import java.util.Properties;

public abstract class StatelessStreamProcessor extends StreamProcessor {

    public StatelessStreamProcessor(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
        super(props, producerProps, consumerProps);
    }

    // it does not save anything
    @Override
    public void saveState() {
        // it does nothing since it has no state to save
    }

}
