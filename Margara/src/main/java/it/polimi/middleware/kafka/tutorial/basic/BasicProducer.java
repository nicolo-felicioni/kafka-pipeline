package it.polimi.middleware.kafka.tutorial.basic;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class BasicProducer {
	private static final boolean print = true;
	private static final boolean waitAck = false;
	private static final int waitBetweenMsgs = 500;
	private static final boolean idempotent = false;

	public static void main(String[] args) {
		if (args.length < 1) {
			err();
		}
		final List<String> topics = Arrays.asList(args);
		final int numMessages = 100000;

		final Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());
		props.put("enable.idempotence", idempotent);

		final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		final Random r = new Random();

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
			final Future<RecordMetadata> future = producer.send(record);

			if (waitAck) {
				try {
					System.out.println(future.get());
				} catch (InterruptedException | ExecutionException e1) {
					e1.printStackTrace();
				}
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
		System.out.println("Usage: BasicProducer <topic> [<topic>*]");
		System.exit(1);
	}

}
