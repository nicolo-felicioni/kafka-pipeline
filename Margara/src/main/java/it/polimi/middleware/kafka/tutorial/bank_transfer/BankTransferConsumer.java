package it.polimi.middleware.kafka.tutorial.bank_transfer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BankTransferConsumer {
	public static void main(String[] args) throws InterruptedException {
		final String group = "group";
		final Map<String, Integer> accounts = new HashMap<>();

		final int numConsumers = Integer.parseInt(args[0]);

		final Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", group);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());

		final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
		for (int i = 0; i < numConsumers; i++) {
			executor.submit(new BankTransferConsumerRunnable(props, accounts));
		}
		executor.shutdown();
		while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
		}
	}
}

class BankTransferConsumerRunnable implements Runnable {
	private static final String topic = "bank";

	private final KafkaConsumer<String, String> consumer;
	private final Map<String, Integer> accounts;
	private volatile boolean running;

	public BankTransferConsumerRunnable(Properties props, Map<String, Integer> accounts) {
		this.accounts = accounts;
		this.consumer = new KafkaConsumer<>(props);
		running = true;
	}

	@Override
	public void run() {
		try {
			consumer.subscribe(Collections.singletonList(topic));
			while (running) {
				final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
				for (final ConsumerRecord<String, String> record : records) {
					final String value = record.value();
					final String from = value.split("#")[0];
					final String to = value.split("#")[1];
					final int amount = Integer.valueOf(value.split("#")[2]);

					synchronized (accounts) {
						if (Integer.valueOf(to) == -1) {
							final Integer val = accounts.getOrDefault(from, 0);
							if (val + amount > 0) {
								accounts.put(from, val + amount);
							}
						} else {
							final Integer fromVal = accounts.getOrDefault(from, 0);
							final Integer toVal = accounts.getOrDefault(from, 0);
							if (fromVal - amount > 0) {
								accounts.put(from, fromVal - amount);
								accounts.put(to, toVal + amount);
							}
						}
					}
				}
			}
		} finally {
			consumer.close();
		}
	}

	public void shutdown() {
		running = false;
	}
}