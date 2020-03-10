package it.polimi.middleware.kafka.tutorial.bank;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class BankProducer {
	private static final boolean print = true;
	private static final boolean waitAck = false;
	private static final int waitBetweenMsgs = 500;

	public static void main(String[] args) {
		final String topic = "bank";
		final int numMessages = 100000;
		final int numAccounts = 1000;
		final int maxAmount = 100;

		final Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.1.8:32786");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());

		final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		final Random r = new Random();

		for (int i = 0; i < numMessages; i++) {
			final int account = r.nextInt(numAccounts);
			final int amount = 1 + r.nextInt(maxAmount);

			final String key = "Key" + account;
			final String value = account + "#" + amount;
			if (print) {
				System.out.println("Topic: " + topic + "\t" + //
				    "Key: " + key + "\t" + //
				    "Value: " + value);
			}

			final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
			final Future<RecordMetadata> future = producer.send(record);

			if (waitAck) {
				try {
					System.out.println(future.get());
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
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

}
