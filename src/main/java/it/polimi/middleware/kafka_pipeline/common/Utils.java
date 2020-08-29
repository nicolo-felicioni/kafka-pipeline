package it.polimi.middleware.kafka_pipeline.common;

import it.polimi.middleware.kafka_pipeline.processors.*;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.lang.ref.PhantomReference;
import java.util.Properties;

public class Utils {

    public static Properties getConsumerProperties() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", Config.SERVER_IP+":"+Config.SERVER_PORT);
        consumerProps.put("group.id", Config.GROUP);
        consumerProps.put("partition.assignment.strategy", RoundRobinAssignor.class.getName());
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("isolation.level", "read_committed");
        consumerProps.put("enable.auto.commit", "false");

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
        producerProps.put("acks", "all");
        producerProps.put("enable.idempotence", true);

        return producerProps;
    }

    public static ProcessorType getProcessorType(String type) {
        if (type.equals("forward"))
            return ProcessorType.FORWARD;
        else if (type.equals("sum"))
            return ProcessorType.SUM;
        else if (type.equals("count"))
            return ProcessorType.COUNT;
        else if (type.equals("average"))
            return ProcessorType.AVERAGE;
        else
            return ProcessorType.UNKNOWN;
    }

    public static StreamProcessor getProcessorByType(StreamProcessorProperties props) {
        Properties producerProps = getProducerProperties();
        Properties consumerProps = getConsumerProperties();
        ProcessorType type = props.getType();

        // TODO : FILL THIS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        if (type == ProcessorType.FORWARD) {
            return new Forwarder(props, producerProps, consumerProps);
        }
        else if (type == ProcessorType.SUM) {
            return new Sum(props, producerProps, consumerProps);
        }
        else if (type == ProcessorType.COUNT) {
            return new Count(props, producerProps, consumerProps);
        }
        else if (type == ProcessorType.AVERAGE) {
            return null;
        }
        else {
            return null;
        }
    }

}
