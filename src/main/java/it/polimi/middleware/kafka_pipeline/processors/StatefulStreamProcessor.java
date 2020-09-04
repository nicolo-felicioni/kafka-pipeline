package it.polimi.middleware.kafka_pipeline.processors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class StatefulStreamProcessor extends StreamProcessor {


    public StatefulStreamProcessor(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
        super(props, producerProps, consumerProps);
        Properties stateConsumerProps = (Properties) consumerProps.clone();
        // TODO find ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
        // stateConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> stateConsumer = new KafkaConsumer<>(stateConsumerProps);
        // TODO static assignment
        stateConsumer.assign(Collections.singleton(new TopicPartition(props.getStateTopic(), 0)));
        resumeFromState(stateConsumer);
    }

    abstract void resumeFromState(KafkaConsumer<String, String> stateConsumer);


}
