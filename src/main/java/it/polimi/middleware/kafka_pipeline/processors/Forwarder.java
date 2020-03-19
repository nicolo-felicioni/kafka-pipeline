package it.polimi.middleware.kafka_pipeline.processors;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Forwarder extends StreamProcessor {

	public Forwarder(String id, String type, String from, String to, Properties producerProps, Properties consumerProps) {
		super(id, type, from, to, producerProps, consumerProps);
	}

	@Override
	public ConsumerRecords<String, String> executeOperation(ConsumerRecords<String, String> records) {
		//simple forward
		return records;
	}

	@Override
	public StreamProcessor clone() {
		return null;
	}
}