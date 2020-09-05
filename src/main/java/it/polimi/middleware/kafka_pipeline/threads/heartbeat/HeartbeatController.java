package it.polimi.middleware.kafka_pipeline.threads.heartbeat;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class HeartbeatController extends Thread {

    private Map<Integer, Integer> heartbeats;
    private KafkaConsumer<String, String> heartbeatConsumer;
    private Boolean running;
    private boolean firstRound = true;

    public HeartbeatController(int tmNumber) {
        System.out.println("JobManager : creating HeartbeatController with task managers number : " + tmNumber);

        this.heartbeats = new HashMap<>();

        for (int i = 0; i < tmNumber; i++) {
            heartbeats.put(i, 0);
        }

        /*Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.out.println("Caught " + e);
            }
        });*/

        heartbeatConsumer = new KafkaConsumer<>(Utils.getConsumerProperties());
        heartbeatConsumer.assign(Collections.singleton(new TopicPartition(Config.HEARTBEAT_TOPIC, 0)));
    }

    @Override
    public void run() {

        running = true;

        KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.getProducerProperties());

        while(running) {

            // update task managers counter
            heartbeats.replaceAll((k, v) -> v + 1);

            ConsumerRecords<String, String> records = heartbeatConsumer.poll(Duration.of(2, ChronoUnit.SECONDS));
            // update heartbeat for each task manager
            for (ConsumerRecord<String, String> record : records) {
                heartbeats.put(Integer.parseInt(record.key()), 0);
                System.out.println("HeartbeatController: received heartbeat from TaskManager " + record.key());
            }

            // check if task managers are alive
            int count;
            if (firstRound) {
                count = 10;
                firstRound = false;
            }
            else {
                count = 5;
            }

            System.out.println("COUNT: " + count);

            for (int k : heartbeats.keySet()) {
                if (heartbeats.get(k) == count) {
                    System.out.println("HeartbeatController: TaskManager " + k + " is down");
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(Config.HEARTBEAT_EVENTS_TOPIC, String.valueOf(k), "down");
                    producer.send(record);
                }
            }

            System.out.println("HeartbeatController: heartbeat counters " + heartbeats);
        }
    }

    public Map<Integer, Integer> getHeartbeats() {
        return heartbeats;
    }

    @Override
    public void interrupt() {
        this.running = false;
    }
}
