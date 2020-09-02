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
    KafkaConsumer<String, String> processorsConsumer;
    private boolean running = false;

    public TaskManager(int id, int threadsNum) {
        this.id = id;
        this.threadsNum = threadsNum;
        System.out.println("Available threads number: " + this.threadsNum);
        this.processors = new ArrayList<>();
        this.threads = new ArrayList<>();

        this.processorsConsumer = new KafkaConsumer<>(Utils.getConsumerProperties());
        processorsConsumer.assign(Collections.singleton(
                new TopicPartition(Config.PROCESSORS_TOPIC + "_" + this.id, 0)));

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
        consumer.assign(Collections.singleton(new TopicPartition(Config.THREADS_JM_TOPIC, 0)));
        boolean start = false;
        do {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(500, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, String> r : records) {
                if (Integer.parseInt(r.key()) == this.id && r.value().equals("sos")) {
                    System.out.println("Starting settings");
                    start = true;
                }
            }

        } while(!start);

        consumer.close();
    }

    public void sendThreadsNumber() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.getProducerProperties());

        System.out.println("Sending available threads number");

        ProducerRecord<String, String> record =
                new ProducerRecord<>(Config.THREADS_TM_TOPIC, String.valueOf(this.id), String.valueOf(this.threadsNum));

        producer.send(record);

        producer.close();
    }

    public List<StreamProcessor> waitSerializedPipeline() {
        List<StreamProcessorProperties> processorProperties = receiveProcessorsProperties();
        return createProcessors(processorProperties);
    }

    private List<StreamProcessorProperties> receiveProcessorsProperties() {
        JsonPropertiesSerializer serializer = new JsonPropertiesSerializer();
        List<StreamProcessorProperties> processorsProperties = new ArrayList<>();

        boolean stop = false;
        do {
            ConsumerRecords<String, String> records = processorsConsumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            for (ConsumerRecord<String, String> r : records) {
                if (Integer.parseInt(r.key()) == this.id) {
                    if (r.value().equals("eos")) {
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

    private List<StreamProcessor> createProcessors(List<StreamProcessorProperties> properties) {
        List<StreamProcessor> processorsList = new ArrayList<>();
        for (StreamProcessorProperties props : properties) {
            processorsList.add(Utils.getProcessorByType(props));
        }
        System.out.println("Created processors: " + processorsList);
        return processorsList;
    }

    private void assignProcessors(List<StreamProcessor> processorsList) {
        // round robin assignment of processors to threads
        int thread_index = 0;
        for(int i = 0; i < processorsList.size(); i++) {
            StreamProcessor p = processorsList.get(i);
            this.processors.add(p);
            //System.out.println(p.getProperties());
            System.out.println("TaskManager " + id + " : assigning to Thread " + thread_index + " processor " + p);
            threads.get(thread_index).assign(p);
            thread_index = (thread_index + 1) % threadsNum;
        }
    }

    public void start() {
        this.createThreads();
        this.waitStartSettings();
        this.sendThreadsNumber();
        List<StreamProcessor> processorsList = this.waitSerializedPipeline();

        this.heartbeatThread.start();
        this.assignProcessors(processorsList);

        //System.out.println("task manager processors: " + this.processors);

        for (PipelineThread t : threads) {
            t.start();
        }

        this.run();
    }

    private void run() {
        this.running = true;

        while(this.running) {
            List<StreamProcessor> newProcessors = waitSerializedPipeline();
            this.assignProcessors(newProcessors);

            //System.out.println("task manager processors: " + this.processors);
            //System.out.println("thread processors: " + threads.get(0).getProcessors());
        }
    }

    public void stop() {
        for (PipelineThread t : threads)
            t.interrupt();
        this.heartbeatThread.interrupt();
        this.running = false;

    }

    public int getId() {
        return this.id;
    }

}
