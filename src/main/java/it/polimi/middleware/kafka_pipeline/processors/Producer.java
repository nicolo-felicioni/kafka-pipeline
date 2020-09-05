package it.polimi.middleware.kafka_pipeline.processors;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Producer class for debugging purposes.
 *
 * It produces random numbers on the "source_topic",
 * i.e. the input topic to the processing pipeline.
 */
public class Producer {
    private static final boolean print = true;
    private static final int waitBetweenMsgs = 1000;

    public static void main(String[] args) {

        // Parse global configurations
        new Parser();
        Parser.parseConfig();
        Config.printConfiguration();

        final List<String> topics = Collections.singletonList(Config.SOURCE_TOPIC);
        final int numMessages = 100000;

        final Properties props = new Properties();
        props.put("bootstrap.servers", Config.SERVER_IP+":"+Config.SERVER_PORT);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        //props.put("partition.assignment.strategy", RoundRobinAssignor.class.getName());
        // Idempotence = exactly once semantics between producer and partition
        props.put("enable.idempotence", true);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        final Random r = new Random();

        for (int i = 0; i < numMessages; i++) {
            final String topic = topics.get(r.nextInt(topics.size()));
            final String key = String.valueOf(r.nextInt(5));
            final String value = String.valueOf(i);
            if (print) {
                System.out.println("Topic: " + topic + "\t" + //
                        "Key: " + key + "\t" + //
                        "Value: " + value);
            }
            producer.send(new ProducerRecord<>(topic, key, value));

            try {
                Thread.sleep(waitBetweenMsgs);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();

    }

}
