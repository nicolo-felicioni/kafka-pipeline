package it.polimi.middleware.kafka_pipeline.common;

import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Utils {

    public static Properties getConsumerProperties() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", Config.SERVER_IP+":"+Config.SERVER_PORT);
        consumerProps.put("group.id", Config.GROUP);
        consumerProps.put("partition.assignment.strategy", RoundRobinAssignor.class.getName());
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        return consumerProps;
    }

    public static Properties getProducerProperties() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", Config.SERVER_IP+":"+Config.SERVER_PORT);
        producerProps.put("group.id", Config.GROUP);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("key.deserializer", StringDeserializer.class.getName());
        producerProps.put("value.deserializer", StringDeserializer.class.getName());
        return producerProps;
    }

}
