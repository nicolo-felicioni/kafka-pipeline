package it.polimi.middleware.kafka_pipeline.processors;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Basic StreamProcessor.
 * This class implements an atomic forwarder:
 * it simply forwards incoming messages to its "outgoing topic",
 * i.e. to the next StreamProcessor in the pipeline.
 */
public class Forwarder extends StreamProcessor {

	public Forwarder(int taskId, String id, String type, String from, String to, Properties producerProps, Properties consumerProps) {
		super(taskId, id, type, from, to, producerProps, consumerProps);
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