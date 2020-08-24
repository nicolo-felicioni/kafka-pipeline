package it.polimi.middleware.kafka_pipeline.processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class Sum extends StatelessStreamProcessor {

    public Sum(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
        super(props, producerProps, consumerProps);
    }

    @Override
    public ConsumerRecords<String, String> executeOperation(ConsumerRecords<String, String> records) {
        // creates a Map: topicPartition ---> list of consumer records
        Map<TopicPartition, List<ConsumerRecord<String, String>>> resultMap = new HashMap<>();

        // for each record to process
        for (final ConsumerRecord<String, String> record : records) {
            // get the current topic partition
            TopicPartition currTopicPartition = new TopicPartition(record.topic(), record.partition());

            // get the list of records processed until now (if any, or else get an empty list)
            List<ConsumerRecord<String, String>> resultListCurrPartition = resultMap.getOrDefault(currTopicPartition,
                    new ArrayList<>());

            // update the results of the list with the new value := value + 1
            resultListCurrPartition.add(new ConsumerRecord<>(record.topic(), record.partition(),
                    record.offset(), record.key(), String.valueOf(Integer.parseInt(record.value()) + 1)));

            // update the result list for the current topic partition
            resultMap.put(currTopicPartition, resultListCurrPartition);
        }

        return new ConsumerRecords<>(resultMap);
    }

    @Override
    public StreamProcessor clone() {
        return new Sum(props, producerProps, consumerProps);
    }
}
