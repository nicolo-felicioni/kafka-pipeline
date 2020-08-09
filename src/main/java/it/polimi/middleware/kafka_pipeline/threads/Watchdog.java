package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Watchdog extends Thread {

    private Map<Integer, Boolean> heartbeats;
    private KafkaConsumer<Integer, Boolean> heartbeatConsumer;
    private Boolean running;

    public Watchdog(List<TaskManager> taskManagers) {
        for (TaskManager tm : taskManagers) {
            heartbeats.put(tm.getId(), false);
        }

        heartbeatConsumer = new KafkaConsumer<>(Utils.getConsumerProperties());
        heartbeatConsumer.subscribe(Collections.singletonList(Config.HEARTBEAT_TOPIC));
    }

    @Override
    public void run() {

        System.out.println("Starting heartbeat thread");
        running = true;

        while(running) {
            ConsumerRecords<Integer, Boolean> records = heartbeatConsumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            // update heartbeat for each task manager
            for (ConsumerRecord<Integer, Boolean> record : records) {
                heartbeats.put(record.key(), record.value());
            }
            // check that all task managers are alive
            for (Integer k : heartbeats.keySet()) {
                if (!heartbeats.get(k)) {
                    System.out.println("Task manager " + k + " is down");
                }
            }
            // reset all to false
            for (Integer k : heartbeats.keySet()) {
                heartbeats.put(k, false);
            }
        }
    }

    public Map<Integer, Boolean> getHeartbeats() {
        return heartbeats;
    }

    @Override
    public void interrupt() {
        this.running = false;
        this.interrupt();
    }
}
