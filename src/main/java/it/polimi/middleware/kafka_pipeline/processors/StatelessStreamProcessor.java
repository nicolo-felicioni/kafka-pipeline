package it.polimi.middleware.kafka_pipeline.processors;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Properties;

public abstract class StatelessStreamProcessor extends StreamProcessor {

    public StatelessStreamProcessor(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
        super(props, producerProps, consumerProps);
    }

    // it does not save anything
    @Override
    public void saveState(String record_key, String record_value) {
        // it does nothing since it has no state to save
    }

}
