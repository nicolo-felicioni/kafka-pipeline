package it.polimi.middleware.kafka.tutorial.bank;

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

public class BankConsumer {
	private static final boolean print = true;

	public static void main(String[] args) throws InterruptedException {
		if (args.length < 1) {
			//err();
		}

		final String group = "group";
		final Map<String, Integer> accounts = new HashMap<>();
		final int numConsumers = 5;

		final Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.1.8:32786");
		props.put("group.id", group);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());

		final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
		for (int i = 0; i < numConsumers; i++) {
			executor.submit(new BankConsumerRunnable(props, accounts, print));
		}
		executor.shutdown();
		while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
		}
	}

	private static final void err() {
		System.out.println("Usage: BankConsumer <numConsumers>");
		System.exit(1);
	}
}

class BankConsumerRunnable implements Runnable {
	private static final String topic = "bank";

	private final KafkaConsumer<String, String> consumer;
	private final Map<String, Integer> accounts;
	private volatile boolean running;
	private final boolean print;

	public BankConsumerRunnable(Properties props, Map<String, Integer> accounts, boolean print) {
		this.accounts = accounts;
		this.consumer = new KafkaConsumer<>(props);
		running = true;
		this.print = print;
	}

	@Override
	public void run() {
		try {
			consumer.subscribe(Collections.singletonList(topic));
			while (running) {
				final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
				for (final ConsumerRecord<String, String> record : records) {
					final String value = record.value();
					final String account = value.split("#")[0];
					final int amount = Integer.valueOf(value.split("#")[1]);
					int val = 0;
					// It is safe to synchronize twice: no operation can occur between the
					// two synchronizations, since only one consumer is associated to a
					// given key
					synchronized (accounts) {
						val = accounts.getOrDefault(account, 0);
					}
					if (val + amount > 0) {
						if (print) {
							System.out.println("Account: " + account + ". Old value: " + val + ". New value: " + (val + amount));
						}
						synchronized (accounts) {
							accounts.put(account, val + amount);
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
