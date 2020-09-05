package it.polimi.middleware.kafka_pipeline.processors;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Basic StreamProcessor.
 * This class implements an atomic forwarder:
 * it simply forwards incoming messages to its "outgoing topic",
 * i.e. to the next StreamProcessor in the pipeline.
 */
public class Forwarder extends StatelessStreamProcessor {

	public Forwarder(StreamProcessorProperties props, Properties producerProps, Properties consumerProps) {
		super(props, producerProps, consumerProps);
	}

	@Override
	public List<ProducerRecord<String, String>> executeOperation(ConsumerRecords<String, String> records) {
		//simple forward
		List<ProducerRecord<String,String>> results = new ArrayList<>();
		for (ConsumerRecord<String,String> record : records)
			results.add(new ProducerRecord<>(record.topic(), record.key(),record.value()));

		return results;
	}

	@Override
	public StreamProcessor clone() {
		return null;
	}
}