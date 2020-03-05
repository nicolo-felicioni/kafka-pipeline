package it.polimi.middleware.kafka_pipeline.processors.sinks;

import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.processors.sources.MockSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class MockSink extends StreamProcessor {

    private Random r = new Random();

    public MockSink(String id, String type, String from, String to,
                      Properties producerProps, Properties consumerProps) {
        super(id, type, from, to, producerProps, consumerProps);
        this.consumer = new KafkaConsumer<>(producerProps);
        consumer.subscribe(Collections.singleton(inTopic));
        System.out.println("Sink created");
    }

    @Override
    public ConsumerRecords<String, String> receive() {
        return consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
    }

    @Override
    public void send(ProducerRecord<String, String> record) {
        // do nothing
    }

    @Override
    public void process() {
        ConsumerRecords<String, String> records = receive();
        for (final ConsumerRecord<String, String> record : records) {
            System.out.println("Consumed : " + record.key() + " : " + record.value());
        }
    }

    @Override
    public StreamProcessor clone() {
        return new MockSink(id, type, from, to, (Properties)producerProps.clone(), (Properties)consumerProps.clone());
    }
}
