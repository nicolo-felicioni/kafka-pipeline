package it.polimi.middleware.kafka.tutorial.basic;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ManualCommitConsumer {
	public static void main(String[] args) throws InterruptedException {
		if (args.length < 4) {
			err();
		}

		final String group = args[0];
		final int initialId = Integer.parseInt(args[1]);
		final int numConsumers = Integer.parseInt(args[2]);
		final List<String> topics = Arrays.asList(Arrays.copyOfRange(args, 3, args.length));

		final Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", group);
		props.put("enable.auto.commit", "false");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());

		final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
		for (int i = 0; i < numConsumers; i++) {
			final int id = initialId + i;
			executor.submit(new ManualCommitConsumerRunnable(id, props, topics));
		}
		executor.shutdown();
		while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
		}
	}

	private static final void err() {
		System.out.println("Usage: ManualCommitConsumer <group> <initialId> <numConsumers> <topic> [<topic>*]");
		System.exit(1);
	}
}

class ManualCommitConsumerRunnable implements Runnable {
	private final int id;
	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private volatile boolean running;

	public ManualCommitConsumerRunnable(int id, Properties props, List<String> topics) {
		this.id = id;
		this.topics = topics;
		this.consumer = new KafkaConsumer<>(props);
		running = true;
	}

	@Override
	public void run() {
		try {
			System.out.println("Topics: " + topics);
			consumer.subscribe(topics);
			while (running) {
				final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
				for (final ConsumerRecord<String, String> record : records) {
					System.out.println("Consumer: " + id + ".\t" + //
					    "Partition: " + record.partition() + ".\t" + //
					    "Offset: " + record.offset() + ".\t" + //
					    "Key: " + record.key() + ".\t" + //
					    "Value: " + record.value());

					// There is also an asynchronous version that invokes a callback
					consumer.commitSync();
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