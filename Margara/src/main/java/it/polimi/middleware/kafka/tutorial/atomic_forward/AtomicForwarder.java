package it.polimi.middleware.kafka.tutorial.atomic_forward;

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

public class AtomicForwarder {

	private static final int numRepetitions = 1000000;

	public static void main(String[] args) {
		final String group = "consumerGroup";
		final String inTopic = "topic1";
		final String outTopic = "topic2";

		final Properties consumerProps = new Properties();
		consumerProps.put("bootstrap.servers", "localhost:9092");
		consumerProps.put("group.id", group);
		consumerProps.put("key.deserializer", StringDeserializer.class.getName());
		consumerProps.put("value.deserializer", StringDeserializer.class.getName());
		consumerProps.put("isolation.level", "read_committed");
		consumerProps.put("enable.auto.commit", "false");

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
		consumer.subscribe(Collections.singleton(inTopic));

		final Properties producerProps = new Properties();
		producerProps.put("bootstrap.servers", "localhost:9092");
		producerProps.put("group.id", group);
		producerProps.put("key.serializer", StringSerializer.class.getName());
		producerProps.put("value.serializer", StringSerializer.class.getName());
		producerProps.put("transactional.id", "transaction_id");

		final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
		producer.initTransactions();

		for (int i = 0; i < numRepetitions; i++) {
			final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
			producer.beginTransaction();
			for (final ConsumerRecord<String, String> record : records) {
				System.out.println(record.key() + " : " + record.value());
				producer.send(new ProducerRecord<>(outTopic, record.key(), record.value()));
			}

			// The producer manually commits the outputs for the consumer within the
			// transaction
			final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
			for (final TopicPartition partition : records.partitions()) {
				final List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
				final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
				map.put(partition, new OffsetAndMetadata(lastOffset + 1));
			}

			producer.sendOffsetsToTransaction(map, group);
			producer.commitTransaction();
		}

		consumer.close();
		producer.close();

	}

}