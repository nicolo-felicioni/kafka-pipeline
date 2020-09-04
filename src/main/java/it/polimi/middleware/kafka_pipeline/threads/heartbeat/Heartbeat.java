package it.polimi.middleware.kafka_pipeline.threads.heartbeat;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Heartbeat extends Thread {

    private int taskManagerID;
    private KafkaProducer<String, String> heartbeatProducer;
    private Boolean running;

    public Heartbeat(int taskManagerID) {
        this.taskManagerID = taskManagerID;
        this.heartbeatProducer = new KafkaProducer<>(Utils.getProducerProperties());
    }

    @Override
    public void run() {

        System.out.println("Starting heartbeat thread");
        running = true;

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ProducerRecord<String, String> pingRecord =
                new ProducerRecord<>(Config.HEARTBEAT_TOPIC,
                        String.valueOf(this.taskManagerID), "ping");

        while(running) {
            heartbeatProducer.send(pingRecord);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void interrupt() {
        this.running = false;
        this.interrupt();
    }
}
