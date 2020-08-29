package it.polimi.middleware.kafka_pipeline;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.threads.JobManager;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class MainJobManager {

    public static void main(String[] args) {

        // Parse global configurations
        new Parser();
        Parser.parseConfig();

        Config.printConfiguration();

        TopicsManager topicsManager = TopicsManager.getInstance();
        // create topics
        List<String> topics = Parser.parseTopics();
        topicsManager.setSourceTopic(Config.SOURCE_TOPIC);
        topicsManager.setSinkTopic(Config.SINK_TOPIC);
        topicsManager.createTopics(topics);
        topicsManager.createTopics(Collections.singletonList(Config.HEARTBEAT_TOPIC));

        /*
            TODO : jobmanager should ask task managers how many
                   threads they can handle (avoid strugglers)
         */
        new JobManager().start();

       /* KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(Utils.getConsumerProperties());
        consumer.subscribe(Collections.singletonList(Config.SOURCE_TOPIC));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));

            for (final ConsumerRecord<String, String> result_record : records) {
                System.out.println(result_record.key() + " - " + result_record.value());
            }
        }*/
    }
}
