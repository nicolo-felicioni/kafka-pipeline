package it.polimi.middleware.kafka_pipeline.processors.sources;

import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;


public class MockSource extends StreamProcessor {

    private Random r = new Random();

    public MockSource(String id, String type, String from, String to,
                      Properties producerProps, Properties consumerProps) {
        super(id, type, from, to, producerProps, consumerProps);
        this.producer = new KafkaProducer<>(producerProps);
        System.out.println("Source created");
    }

    @Override
    public ConsumerRecords<String, String> receive() {
        return null;
    }

    @Override
    public void send(ProducerRecord<String, String> record) {
        producer.send(record);
    }

    @Override
    public void process() {
        String key = "mock_source_key:" + r.nextInt(100);
        String value = "value:" + r.nextInt(100);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                                                        outTopic,
                                                        key,
                                                        value
                                                );
        System.out.println("Produced : " + record.key() + " : " + record.value());
        this.send(record);
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public StreamProcessor clone() {
        return new MockSource(id, type, from, to, (Properties)producerProps.clone(), (Properties)consumerProps.clone());
    }
}
