package it.polimi.middleware.kafka_pipeline.threads.heartbeat;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.threads.TaskManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class HeartbeatController extends Thread {

    private Map<Integer, Integer> heartbeats;
    private KafkaConsumer<String, String> heartbeatConsumer;
    private Boolean running;

    public HeartbeatController(List<TaskManager> taskManagers) {
        System.out.print("JobManager : creating HeartbeatController with TaskManagers : " + taskManagers);

        this.heartbeats = new HashMap<>();

        for (TaskManager tm : taskManagers) {
            heartbeats.put(tm.getId(), 0);
        }

        heartbeatConsumer = new KafkaConsumer<>(Utils.getConsumerProperties());
        heartbeatConsumer.subscribe(Collections.singletonList(Config.HEARTBEAT_TOPIC));
    }

    @Override
    public void run() {

        running = true;

        while(running) {

            System.out.println("HeartbeatController: heartbeat counters " + heartbeats);

            // update task managers counter
            for (int k : heartbeats.keySet()) {
                heartbeats.put(k, heartbeats.get(k)+1);
            }

            ConsumerRecords<String, String> records = heartbeatConsumer.poll(Duration.of(2, ChronoUnit.SECONDS));
            // update heartbeat for each task manager
            for (ConsumerRecord<String, String> record : records) {
                heartbeats.put(Integer.parseInt(record.key()), 0);
                System.out.println(record.key());
            }

            // check if task managers are alive
            for (int k : heartbeats.keySet()) {
                if (heartbeats.get(k) == 3) {
                    System.out.println("HeartbeatController: TaskManager " + k + " is down");
                }
            }
        }
    }

    public Map<Integer, Integer> getHeartbeats() {
        return heartbeats;
    }

    @Override
    public void interrupt() {
        this.running = false;
        this.interrupt();
    }
}
