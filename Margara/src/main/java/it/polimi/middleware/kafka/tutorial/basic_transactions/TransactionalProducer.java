package it.polimi.middleware.kafka.tutorial.basic_transactions;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TransactionalProducer {
	private static final boolean print = true;
	private static final int waitBetweenMsgs = 500;

	public static void main(String[] args) {
		if (args.length < 1) {
			err();
		}
		final List<String> topics = Arrays.asList(args);
		final int numMessages = 100000;

		final Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		// Transactional ID is used for fault tolerance:
		// to identify the "same" producer if it crashes and gets restarted
		props.put("transactional.id", "my-transactional-id");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());

		final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		final Random r = new Random();

		// This must be called before any method that involves transactions
		producer.initTransactions();

		for (int i = 0; i < numMessages; i++) {
			final String topic = topics.get(r.nextInt(topics.size()));
			final String key = "Key" + r.nextInt(1000);
			final String value = "Val" + i;
			if (print) {
				System.out.println("Topic: " + topic + "\t" + //
				    "Key: " + key + "\t" + //
				    "Value: " + value + "\t");
			}

			final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

			producer.beginTransaction();
			producer.send(record);
			if (i % 2 == 0) {
				producer.commitTransaction();
			} else {
				// If not flushed, aborted messages are deleted from the outgoing buffer
				producer.flush();
				producer.abortTransaction();
			}

			try {
				Thread.sleep(waitBetweenMsgs);
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
		}

		producer.close();
	}

	private static final void err() {
		System.out.println("Usage: TransactionalProducer <topic> [<topic>*]");
		System.exit(1);
	}

}
