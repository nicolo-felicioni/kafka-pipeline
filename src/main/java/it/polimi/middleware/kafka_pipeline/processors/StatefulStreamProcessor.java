package it.polimi.middleware.kafka_pipeline.processors;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public abstract class StatefulStreamProcessor extends StreamProcessor {


    public StatefulStreamProcessor(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
        super(props, producerProps, consumerProps);
    }

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
