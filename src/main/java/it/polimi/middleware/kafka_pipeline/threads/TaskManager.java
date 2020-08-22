package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.JsonPropertiesSerializer;
import it.polimi.middleware.kafka_pipeline.common.ProcessorType;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessorProperties;
import it.polimi.middleware.kafka_pipeline.threads.heartbeat.Heartbeat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TaskManager {

    private int id;
    private int threads_num = 1; //min(Runtime.getRuntime().availableProcessors(), this.processors.size());
    private List<StreamProcessor> processors;
    private List<PipelineThread> threads;
    private Heartbeat heartbeatThread;

    public TaskManager(int id) {
        this.id = id;
        this.processors = new ArrayList<>();
        this.threads = new ArrayList<>();
    }

    /**
     * Create threads, assign them to tasks and start executing them.
     */
    public void createThreads() {

        this.heartbeatThread = new Heartbeat(this.id);

        for (int i = 0; i < threads_num; i++)
            threads.add(new PipelineThread(i, this.id));
    }

    public void waitSerializedPipeline() {
        List<StreamProcessorProperties> processorProperties = receiveProcessorsProperties();
        createProcessors(processorProperties);
    }

    private List<StreamProcessorProperties> receiveProcessorsProperties() {
        JsonPropertiesSerializer serializer = new JsonPropertiesSerializer();
        List<StreamProcessorProperties> processorsProperties = new ArrayList<>();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Utils.getConsumerProperties());
        consumer.subscribe(Collections.singletonList(Config.SETTINGS_TOPIC + "_" + this.id));

        boolean stop = false;
        do {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            for (ConsumerRecord<String, String> r : records) {
                if (Integer.parseInt(r.key()) == this.id) {
                    if (r.value().equals("stop")) {
                        stop = true;
                    }
                    else {
                        StreamProcessorProperties props = serializer.deserialize(r.value());
                        System.out.println("Received: " + props);
                        processorsProperties.add(props);
                    }
                }
            }

        } while(!stop);

        return processorsProperties;
    }

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
            thread_index = (thread_index + 1) % threads_num;
        }
    }

    public void start() {
        assignProcessors();
        for (PipelineThread t : threads)
            t.start();
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
