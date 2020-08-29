package it.polimi.middleware.kafka_pipeline.processors;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class StatefulStreamProcessor extends StreamProcessor {

    Map<String, String> state;

    public StatefulStreamProcessor(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
        super(props, producerProps, consumerProps);
        this.state = new HashMap<>();
        KafkaConsumer<String, String> stateConsumer = new KafkaConsumer<>(consumerProps);
        // TODO static assignment
        stateConsumer.assign(Collections.singleton(new TopicPartition(props.getStateTopic(), 0)));
        resumeFromState(stateConsumer);
    }

    abstract void resumeFromState(KafkaConsumer<String, String> stateConsumer);

    // it forwards all the elements to the state topic
    @Override
    public void saveState(String record_key, String record_value) {
        {

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(props.getStateTopic(), record_key, record_value);

            System.out.println("ID: " + this.getId() + " " + props.getPipelineID() + " - Transaction_ID: " + this.producerProps.getProperty("transactional.id") + " - Saved state: topic:" + record.topic() + " - key:" + record.key() + " - value:" + record.value());

            producer.send(record);
        }
    }

}
