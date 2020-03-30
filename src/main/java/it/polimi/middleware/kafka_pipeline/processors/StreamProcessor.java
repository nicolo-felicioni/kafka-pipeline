package it.polimi.middleware.kafka_pipeline.processors;

import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;
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

public abstract class StreamProcessor {

    protected String id;
    protected String type;
    protected String from;
    protected String to;
    protected String inTopic;
    protected String outTopic;

    protected KafkaProducer<String, String> producer;
    protected KafkaConsumer<String, String> consumer;

    protected Properties consumerProps;
    protected Properties producerProps;

    public StreamProcessor(String id, String type, String from, String to,
                                    Properties producerProps, Properties consumerProps) {
        this.id = id;
        this.type = type;
        this.from = from;
        this.to = to;


        this.inTopic = TopicsManager.getInputTopic(from, id);
        this.outTopic = TopicsManager.getOutputTopic(id, to);

        this.producerProps = producerProps;


        //todo temporary solution for transaction_id
        final String transaction_id = id + "_" + to + "_"; // + task_id
        this.producerProps.put("transactional.id", transaction_id);

        this.consumerProps = consumerProps;

        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(Collections.singletonList(this.inTopic));

        this.producer = new KafkaProducer<>(producerProps);

        /*
         * The method initTransactions needs to be called before
         * any other methods when the transactional.id is set in the configuration.
         * */
        producer.initTransactions();

    }

    public String getId() {
        return id;
    }

    public String getInputTopic() {
        return inTopic;
    }

    public String getOutputTopic() {
        return outTopic;
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

    public void send(ProducerRecord<String, String> record) {
        System.out.println(this.getId() + " - Produced: topic:" + record.topic() + " - key:" + record.key() + " - value:" + record.value());
        producer.send(record);
    }

    public void process(){
        ConsumerRecords<String, String> results;

        producer.beginTransaction();

        //receive the inputs
        ConsumerRecords<String, String> records = receive();

        //get the results from the operation
        results = executeOperation(records);



        //for every result, write it in the outTopic
        for (final ConsumerRecord<String, String> result_record : results) {
            System.out.println(this.getId() + " - Consumed: topic:" + result_record.topic() + " - key:" + result_record.key() + " - value:" + result_record.value());
            send(new ProducerRecord<>(outTopic, result_record.key(), result_record.value()));
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
        return "ID: " + id + " - From: " + from + " - To: " + to;
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
