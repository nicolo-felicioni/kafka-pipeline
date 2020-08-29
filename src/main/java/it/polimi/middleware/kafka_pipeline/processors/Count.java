package it.polimi.middleware.kafka_pipeline.processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class Count extends StatefulStreamProcessor{

    public Count(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
        super(props, producerProps, consumerProps);
    }

    @Override
    void resumeFromState(KafkaConsumer<String, String> stateConsumer) {
        ConsumerRecords<String, String> records;
        do{
            records = stateConsumer.poll(Duration.of(1, ChronoUnit.SECONDS));

            for(ConsumerRecord<String, String> record : records){

                // if the value already present in the state map is lt the value found saved in the topic, then update
                // default value : 0
                if(Integer.parseInt(state.getOrDefault(record.key(), "0")) < Integer.parseInt(record.value()))
                    state.put(record.key(), record.value());
            }

        }while(!records.isEmpty());
    }

    @Override
    public ConsumerRecords<String, String> executeOperation(ConsumerRecords<String, String> records) {

        // creates a Map: topicPartition ---> list of consumer records
        Map<TopicPartition, List<ConsumerRecord<String, String>>> resultMap = new HashMap<>();

        for(ConsumerRecord<String, String> record : records){
            // get the current topic partition
            TopicPartition currTopicPartition = new TopicPartition(record.topic(), record.partition());

            // get the list of records processed until now (if any, or else get an empty list)
            List<ConsumerRecord<String, String>> resultListCurrPartition = resultMap.getOrDefault(currTopicPartition,
                    new ArrayList<>());

            // get the current count for this key and update the state
            int count = Integer.parseInt(state.getOrDefault(record.key(), "0"));
            count++;
            state.put(record.key(), String.valueOf(count));

            // update the results of the list with the new value := value + 1
            resultListCurrPartition.add(new ConsumerRecord<>(record.topic(), record.partition(),
                    record.offset(), record.key(), String.valueOf(count)));

            // update the result list for the current topic partition
            resultMap.put(currTopicPartition, resultListCurrPartition);

        }
        return new ConsumerRecords<>(resultMap);
    }


    @Override
    public StreamProcessor clone() {
        return new Count(props, producerProps, consumerProps);
    }
}
