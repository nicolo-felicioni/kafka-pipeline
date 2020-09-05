package it.polimi.middleware.kafka_pipeline.processors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


class Pair<T, V>{
    T first;
    V second;

    public Pair(T first, V second) {
        this.first = first;
        this.second = second;
    }
}

public abstract class StatefulStreamProcessor extends StreamProcessor {


    public StatefulStreamProcessor(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
        super(props, producerProps, consumerProps);
        Properties stateConsumerProps = (Properties) consumerProps.clone();

        // I want to read all the messages on the state topic
        stateConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> stateConsumer = new KafkaConsumer<>(stateConsumerProps);
        stateConsumer.assign(Collections.singleton(new TopicPartition(props.getStateTopic(), props.getPipelineID())));
        resumeFromState(stateConsumer);
        stateConsumer.close();
    }

    abstract void resumeFromState(KafkaConsumer<String, String> stateConsumer);


}
