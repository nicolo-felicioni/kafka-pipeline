package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.JsonPropertiesSerializer;
import it.polimi.middleware.kafka_pipeline.common.TaskManagerIsDownException;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessorProperties;
import it.polimi.middleware.kafka_pipeline.threads.heartbeat.HeartbeatController;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.sql.SQLOutput;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class JobManager {

    private HeartbeatController heartbeatController;
    private LoadBalancer loadBalancer;
    private int tmNumber;
    private int aliveTmNumber;
    private List<List<StreamProcessorProperties>> tmProcessors;
    private boolean running = false;

    public JobManager() {
        this.tmNumber = Config.TM_NUMBER;
        this.aliveTmNumber = this.tmNumber;

        this.loadBalancer = new LoadBalancer();
        this.tmProcessors = this.createTMProcessorsLists();
    }

    public void start() {

        System.out.println("JobManager : starting heartbeat controller thread");
        this.heartbeatController = new HeartbeatController(this.tmNumber);
        this.heartbeatController.start();

        this.setup();

        running = true;

        //this.sendStartSignal();

        while (running) {
            try {
                if (this.aliveTmNumber == 0) {
                    System.out.println("JobManager: no alive TaskManager exists");
                    this.stop();
                }
            }
            catch (TaskManagerIsDownException e) {
                System.out.println(e.getMessage());
                this.aliveTmNumber--;
                this.tmProcessors = this.loadBalancer.rebalanceProcessors(e.getTaskManagerDownID(),
                                                                            this.aliveTmNumber,
                                                                            this.tmProcessors);
            }
        }

    }

    public void stop() {
        running = false;
    }

    private void setup() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.getProducerProperties());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Utils.getConsumerProperties());
        consumer.assign(Collections.singleton(new TopicPartition(Config.SETTING_THREADS_TOPIC, 0)));

        System.out.println("JobManager: start settings");

        for (int i = 0; i < this.tmNumber; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(Config.SETTING_THREADS_TOPIC, String.valueOf(i), "start_settings");

            producer.send(record);
        }

        // get threads number from all the task managers
        Map<Integer, Integer> threadsNumbersMap = new HashMap<>();
        while(threadsNumbersMap.size() != this.tmNumber) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            for (final ConsumerRecord<String, String> r : records) {
                threadsNumbersMap.put(Integer.parseInt(r.key()), Integer.parseInt(r.value()));
                System.out.println("JobManager: TaskManager " + r.key() + " has " + r.value() + " threads");
            }
        }
        System.out.println("JobManager: threads number map " + threadsNumbersMap);


        this.loadBalancer.setThreadsNumbersMap(threadsNumbersMap);
        List<List<StreamProcessorProperties>> pipelines = createPipelines();
        this.tmProcessors = this.loadBalancer.assignProcessors(pipelines, this.tmProcessors);
        this.sendSerializedPipelines(this.tmProcessors);
    }

    private void sendSerializedPipelines(List<List<StreamProcessorProperties>> processors) {
        JsonPropertiesSerializer serializer = new JsonPropertiesSerializer();
        KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.getProducerProperties());

        System.out.println("JobManager: sending json serialized processors properties");

        for (int i = 0; i < tmNumber; i++) {
            List<StreamProcessorProperties> processorsPropertiesForTM = processors.get(i);

            for (StreamProcessorProperties props : processorsPropertiesForTM) {
                String jsonProperties = serializer.serialize(props);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(Config.SETTINGS_TOPIC + "_" + i, String.valueOf(i), jsonProperties);
                producer.send(record);
            }

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(Config.SETTINGS_TOPIC + "_" + i, String.valueOf(i), "stop_settings");
            producer.send(record);
        }

        System.out.println("JobManager: done settings");
    }

    private List<List<StreamProcessorProperties>> createPipelines() {
        // create some pipelines, according to the PARALLELISM parameter
        List<List<StreamProcessorProperties>> pipelines = new ArrayList<>();
        for (int i = 0; i < Config.PARALLELISM; i++){
            List<StreamProcessorProperties> p = Parser.parsePipeline(i);
            pipelines.add(p);
        }
        return pipelines;
    }

    private List<List<StreamProcessorProperties>> createTMProcessorsLists() {
        // create a list of processors for each task manager
        List<List<StreamProcessorProperties>> processors = new ArrayList<>();
        for (int i = 0; i < this.tmNumber; i++) {
            processors.add(new ArrayList<>());
        }
        return processors;
    }
}