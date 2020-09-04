package it.polimi.middleware.kafka_pipeline.processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class Sum extends StatelessStreamProcessor {

    public Sum(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
        super(props, producerProps, consumerProps);
    }

    @Override
    public List<ProducerRecord<String, String>> executeOperation(ConsumerRecords<String, String> records) {

        // creates a Map: topicPartition ---> list of consumer records
        List<ProducerRecord<String, String>> results = new ArrayList<>();

        for(ConsumerRecord<String, String> record : records){

            // the topic field will be ignored during the sending to output topics
            // since output topics are known
            results.add(new ProducerRecord<>(record.topic(), record.key(), String.valueOf(Integer.parseInt(record.value())+1)));

        }
        return results;
    }

    @Override
    public StreamProcessor clone() {
        return new Sum(props, producerProps, consumerProps);
    }
}
