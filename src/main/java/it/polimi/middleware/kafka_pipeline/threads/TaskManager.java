package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.JsonPropertiesSerializer;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessorProperties;
import it.polimi.middleware.kafka_pipeline.threads.heartbeat.Heartbeat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TaskManager {

    private int id;
    private int threadsNum; //min(Runtime.getRuntime().availableProcessors(), this.processors.size());
    private List<StreamProcessor> processors;
    private List<PipelineThread> threads;
    private Heartbeat heartbeatThread;

    public TaskManager(int id, int threadsNum) {
        this.id = id;
        this.threadsNum = threadsNum;
        System.out.println("Available threads number: " + this.threadsNum);
        this.processors = new ArrayList<>();
        this.threads = new ArrayList<>();
    }

    /**
     * Create threads, assign them to tasks and start executing them.
     */
    public void createThreads() {

        this.heartbeatThread = new Heartbeat(this.id);

        for (int i = 0; i < threadsNum; i++)
            threads.add(new PipelineThread(i, this.id));
    }

    public void waitStartSettings() {
        System.out.println("Waiting for JobManager to start setting up the pipeline");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Utils.getConsumerProperties());
        consumer.assign(Collections.singleton(new TopicPartition(Config.SETTING_THREADS_TOPIC, 0)));
        boolean start = false;
        do {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            for (ConsumerRecord<String, String> r : records) {
                if (Integer.parseInt(r.key()) == this.id && r.value().equals("start_settings")) {
                    System.out.println("starting settings");
                    start = true;
                }
            }

        } while(!start);
    }

    public void sendThreadsNumber() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.getProducerProperties());

        System.out.println("Sending available threads number");

        ProducerRecord<String, String> record =
                new ProducerRecord<>(Config.SETTING_THREADS_TOPIC, String.valueOf(this.id), String.valueOf(this.threadsNum));

        producer.send(record);
    }

    public void waitSerializedPipeline() {
        List<StreamProcessorProperties> processorProperties = receiveProcessorsProperties();
        createProcessors(processorProperties);
    }

    private List<StreamProcessorProperties> receiveProcessorsProperties() {
        JsonPropertiesSerializer serializer = new JsonPropertiesSerializer();
        List<StreamProcessorProperties> processorsProperties = new ArrayList<>();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Utils.getConsumerProperties());
        consumer.assign(Collections.singleton(new TopicPartition(Config.SETTINGS_TOPIC + "_" + this.id, 0)));

        boolean stop = false;
        do {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            for (ConsumerRecord<String, String> r : records) {
                if (Integer.parseInt(r.key()) == this.id) {
                    if (r.value().equals("stop_settings")) {
                        stop = true;
                    }
                    else {
                        //System.out.println(r.value());
                        StreamProcessorProperties props = serializer.deserialize(r.value());
                        System.out.println("Received: " + props);
                        processorsProperties.add(props);
                    }
                }
            }

        } while(!stop);

        System.out.println("Done settings");

        return processorsProperties;
    }

    /*public void waitStartSignal() {
        boolean start = false;
        do {
            ConsumerRecords<String, String> records = settings_consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            for (ConsumerRecord<String, String> r : records) {
                if (Integer.parseInt(r.key()) == this.id && r.value().equals("start")) {
                    System.out.println("starting");
                    start = true;
                }
            }

        } while(!start);
    }*/

    private void createProcessors(List<StreamProcessorProperties> properties) {
        for (StreamProcessorProperties props : properties) {
            this.processors.add(Utils.getProcessorByType(props));
        }
        System.out.println("Processors: " + this.processors);
    }

    private void assignProcessors() {
        // round robin assignment of processors to threads
        int thread_index = 0;
        for(int i = 0; i < processors.size(); i++) {
            StreamProcessor p = processors.get(i);
            System.out.println(p.getProperties());
            System.out.println("TaskManager " + id + " : assigning to Thread " + thread_index + " processor " + p);
            threads.get(thread_index).assign(p);
            thread_index = (thread_index + 1) % threadsNum;
        }
    }

    public void start() {
        assignProcessors();
        for (PipelineThread t : threads) {
            t.start();
        }
        this.heartbeatThread.start();
    }

    public void stop() {
        for (PipelineThread t : threads)
            t.interrupt();
        this.heartbeatThread.interrupt();

    }

    public int getId() {
        return this.id;
    }

}
