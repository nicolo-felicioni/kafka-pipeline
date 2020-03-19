package it.polimi.middleware.kafka_pipeline.processors;

import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

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
        this.consumerProps = consumerProps;

        this.consumer = new KafkaConsumer<>(consumerProps);
        this.producer = new KafkaProducer<>(producerProps);

    }

    public String getId() {
        return id;
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
        producer.send(record);
    }

    public void process(){
        ConsumerRecords<String, String> results;

        //TODO transactions
        //producer.initTransactions();

        //receive the inputs
        ConsumerRecords<String, String> records = receive();

        //get the results from the operation
        results = executeOperation(records);

        //for every result, write it in the outTopic
        for (final ConsumerRecord<String, String> result_record : results) {
            send(new ProducerRecord<>(outTopic, result_record.key(), result_record.value()));
        }
    }


    @Override
    public abstract StreamProcessor clone();

    @Override
    public String toString() {
        return "ID: " + id + " - From: " + from + " - To: " + to;
    }
}
