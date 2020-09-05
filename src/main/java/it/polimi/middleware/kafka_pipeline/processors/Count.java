package it.polimi.middleware.kafka_pipeline.processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;


public class Count extends StatefulStreamProcessor{

    /* the state is in the form:
     *  key ----> (count, isModified)
     */
    private Map<String, Pair<Integer, Boolean>> state;


    public Count(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
        super(props, producerProps, consumerProps);
    }

    @Override
    void resumeFromState(KafkaConsumer<String, String> stateConsumer) {
        this.state = new HashMap<>();
        ConsumerRecords<String, String> records;
        do{
            records = stateConsumer.poll(Duration.of(1, ChronoUnit.SECONDS));

            for(ConsumerRecord<String, String> record : records){

                // if the value already present in the state map is lt the value found saved in the topic, then update
                // default value : 0
                if(state.getOrDefault(record.key(), new Pair<>(0, false)).first < Integer.parseInt(record.value()))
                    state.put(record.key(), new Pair<>(Integer.parseInt(record.value()), true));
            }

        }while(!records.isEmpty());
    }

    @Override
    public List<ProducerRecord<String, String>> executeOperation(ConsumerRecords<String, String> records) {

        // creates a Map: topicPartition ---> list of consumer records
        List<ProducerRecord<String, String>> results = new ArrayList<>();

        for(ConsumerRecord<String, String> record : records){

            // get the current count for this key and update the state
            int count = state.getOrDefault(record.key(), new Pair<>(0, false)).first;
            count++;
            state.put(record.key(), new Pair<>(count, true));

            // update the results of the list with the new value := value + 1
            // the topic field will be ignored during the sending to output topics
            // since output topics are known
            results.add(new ProducerRecord<>(record.topic(), record.key(), String.valueOf(count)));

        }
        return results;
    }

    @Override
    public void saveState() {
        for(String k : state.keySet()){
            // if the state element is modified
            if(state.get(k).second){
                ProducerRecord<String, String> stateElement =
                        new ProducerRecord<>(this.props.getStateTopic(), k, String.valueOf(state.get(k).first));
                producer.send(stateElement);
            }
        }
    }


    @Override
    public StreamProcessor clone() {
        return new Count(props, producerProps, consumerProps);
    }
}
