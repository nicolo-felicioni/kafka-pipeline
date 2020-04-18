package it.polimi.middleware.kafka.tutorial.bank_transfer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class BankTransferProducer {
	private static final boolean print = false;
	private static final boolean waitAck = true;
	private static final int waitBetweenMsgs = 500;

	public static void main(String[] args) {
		final String topic = "bank";
		final int numMessages = 100000;
		final int numAccounts = 1000;
		final int maxAmount = 100;

		final Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());

		final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		final Random r = new Random();

		for (int i = 0; i < numMessages; i++) {
			final int from = r.nextInt(numAccounts);
			final int to = r.nextBoolean() ? r.nextInt(numAccounts) : -1;
			final int amount = 1 + r.nextInt(maxAmount);

			final String key = "Key" + r.nextInt(1000);
			final String value = from + "#" + to + "#" + amount;
			if (print) {
				System.out.print("Topic: " + topic + "\t");
				System.out.print("Key: " + key + "\t");
				System.out.print("Value: " + value + "\t");
				System.out.println();
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

}
