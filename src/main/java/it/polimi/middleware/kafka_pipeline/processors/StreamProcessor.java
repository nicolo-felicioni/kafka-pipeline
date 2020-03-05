package it.polimi.middleware.kafka_pipeline.processors;

import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

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

        this.inTopic = getInputTopic();
        this.outTopic = getOutputTopic();

        this.producerProps = producerProps;
        this.consumerProps = consumerProps;
    }

    public String getId() {
        return id;
    }

    public String getInputTopic() {
        return TopicsManager.getTopicName(from, id);
    }

    public String getOutputTopic() {
        return TopicsManager.getTopicName(id, to);
    }

    // implement in subclasses
    public abstract ConsumerRecords<String, String> receive();
    public abstract void send(ProducerRecord<String, String> record);
    public abstract void process();

    public void stop() {
        consumer.close();
        producer.close();
    }

    @Override
    public abstract StreamProcessor clone();

    @Override
    public String toString() {
        return "ID: " + id + " - From: " + from + " - To: " + to;
    }
}
