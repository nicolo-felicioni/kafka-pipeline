package it.polimi.middleware.kafka_pipeline.processors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Superclass describing the pipeline nodes, called StreamProcessors.
 */
public abstract class StreamProcessor {

    private StreamProcessorProperties props;

    protected KafkaProducer<String, String> producer;
    protected KafkaConsumer<String, String> consumer;

    protected Properties consumerProps;
    protected Properties producerProps;

    public StreamProcessor(StreamProcessorProperties props,
                                    Properties producerProps, Properties consumerProps) {
        this.props = props;

        this.producerProps = producerProps;

        // TODO : temporary solution for transactional_id
        final String transactional_id = props.getID() + "_" + props.getPipelineID();
        this.producerProps.put("transactional.id", transactional_id);
        System.out.println(this.producerProps.getProperty("transactional.id"));
        System.out.println("Processor " + getId() + " : tansactional.id = " + transactional_id);

        this.consumerProps = consumerProps;

        this.consumer = new KafkaConsumer<>(consumerProps);
        // Todo statically assign to partitions (maybe)
        this.consumer.subscribe(Collections.singletonList(props.getInputTopic()));

        this.producer = new KafkaProducer<>(this.producerProps);

        /*
         * The method initTransactions needs to be called before
         * any other methods when the transactional.id is set in the configuration.
         * */
        producer.initTransactions();
    }

    public String getId() {
        return props.getID();
    }

    public String getInputTopic() {
        return props.getInputTopic();
    }

    public String getOutputTopic() {
        return props.getInputTopic();
    }

    //strategy method
    public abstract ConsumerRecords<String, String> executeOperation(ConsumerRecords<String, String> records);

    public void stop() {
        consumer.close();
        producer.close();
    }

    synchronized public ConsumerRecords<String, String> receive() {
        return consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
    }

    // this function sends the k,v pair to the output topic
    public void send(String record_key, String record_value) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(props.getOutputTopic(), record_key, record_value);

        System.out.println("ID: " + this.getId() + " " + props.getPipelineID() + " - Transaction_ID: " + this.producerProps.getProperty("transactional.id") + " - Produced: topic:" + record.topic() + " - key:" + record.key() + " - value:" + record.value());

        producer.send(record);
    }

    // this function sends the k,v pair to the state topic
    public void saveState(String record_key, String record_value) {

        ProducerRecord<String, String> record =
                new ProducerRecord<>(props.getStateTopic(), record_key, record_value);

        System.out.println("ID: " + this.getId() + " " + props.getPipelineID() + " - Transaction_ID: " + this.producerProps.getProperty("transactional.id") + " - Saved state: topic:" + record.topic() + " - key:" + record.key() + " - value:" + record.value());

        producer.send(record);
    }

    public void process(){
        ConsumerRecords<String, String> results;

        producer.beginTransaction();

        //receive the inputs
        ConsumerRecords<String, String> records = receive();

        //get the results from the operation
        results = executeOperation(records);

        //for every result:
        //      write it in the outTopic
        //      save it in the stateTopic
        for (final ConsumerRecord<String, String> result_record : results) {
            System.out.println("ID: " + this.getId() + " " + props.getPipelineID() + " - Consumed: topic:" + result_record.topic() + " - key:" + result_record.key() + " - value:" + result_record.value());

            send(result_record.key(), result_record.value());
            saveState(result_record.key(),result_record.value());

        }

        // The producer manually commits the outputs for the
        // consumer within the transaction
        final Map<TopicPartition, OffsetAndMetadata> map = createPartitionOffsetMap(results);

        producer.sendOffsetsToTransaction(map, this.producerProps.getProperty("group.id"));
        producer.commitTransaction();
    }

    @Override
    public abstract StreamProcessor clone();

    @Override
    public String toString() {
        return "ID: " + props.getID() + " - From: " + props.getFrom() + " - To: " + props.getTo();
    }

    private Map<TopicPartition, OffsetAndMetadata> createPartitionOffsetMap(ConsumerRecords<String, String> records){
        final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        for (final TopicPartition partition : records.partitions()) {
            final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            map.put(partition, new OffsetAndMetadata(lastOffset + 1));
        }
        return map;
    }
}
