package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.JsonPropertiesSerializer;
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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class JobManager {

    private HeartbeatController heartbeatController;
    private LoadBalancer loadBalancer;
    private int tmNumber;
    private int aliveTmNumber;
    private Map<Integer,Boolean> aliveTaskManagers;
    private List<List<StreamProcessorProperties>> tmProcessors;
    private boolean running = false;

    public JobManager() {
        this.tmNumber = Config.TM_NUMBER;
        this.aliveTmNumber = 0;
        this.aliveTaskManagers = new HashMap<>();
        this.loadBalancer = new LoadBalancer();
        this.heartbeatController = new HeartbeatController(this.tmNumber);
        this.tmProcessors = this.loadBalancer.createTMProcessorsLists(this.tmNumber);
    }

    public void start() {
        this.createTopics();

        System.out.println("JobManager : starting heartbeat controller thread");
        this.heartbeatController.start();

        this.setup();
        this.run();
    }

    public void stop() {
        running = false;
        this.heartbeatController.interrupt();
    }

    private void run() {
        running = true;

        KafkaConsumer<String, String> heartbeatEventsConsumer = new KafkaConsumer<>(Utils.getConsumerProperties());
        heartbeatEventsConsumer.assign(Collections.singleton(new TopicPartition(Config.HEARTBEAT_EVENTS_TOPIC, 0)));

        while (running) {

            // records contain: <TaskManagerID, "down">
            ConsumerRecords<String, String> records = heartbeatEventsConsumer.poll(Duration.of(500, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, String> r : records) {

                if (r.value().equals("down")) {
                    int taskManagerDownID = Integer.parseInt(r.key());
                    this.aliveTaskManagers.put(taskManagerDownID, false);
                    this.aliveTmNumber--;

                    if (this.aliveTmNumber > 0) {
                        List<List<StreamProcessorProperties>> toBeSentProcessors = this.loadBalancer.rebalanceProcessors(
                                taskManagerDownID,
                                this.aliveTaskManagers,
                                this.tmNumber,
                                this.tmProcessors);

                        System.out.println("New processors assignment: " + this.tmProcessors);
                        System.out.println("Processors to be sent to other TaskManagers: " + toBeSentProcessors);

                        this.sendSerializedPipelines(toBeSentProcessors);
                    }
                }
            }

            if (this.aliveTmNumber == 0) {
                System.out.println("JobManager: no alive TaskManager exists, stopping");
                this.stop();
            }
        }
    }

    private void setup() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.getProducerProperties());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Utils.getConsumerProperties());
        consumer.assign(Collections.singleton(new TopicPartition(Config.THREADS_TM_TOPIC, 0)));

        System.out.println("JobManager: start setting up the pipeline");

        for (int i = 0; i < this.tmNumber; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(Config.THREADS_JM_TOPIC, String.valueOf(i), "sos");

            producer.send(record);
        }

        // get threads number from all the task managers
        Map<Integer, Integer> threadsNumbersMap = new HashMap<>();
        while(threadsNumbersMap.size() != this.tmNumber) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(500, ChronoUnit.MILLIS));
            for (final ConsumerRecord<String, String> r : records) {
                threadsNumbersMap.put(Integer.parseInt(r.key()), Integer.parseInt(r.value()));
                this.aliveTaskManagers.put(Integer.parseInt(r.key()), true);
                System.out.println("JobManager: TaskManager " + r.key() + " has " + r.value() + " threads");
            }
        }
        //System.out.println("JobManager: threads number for each TaskManager " + threadsNumbersMap);

        this.loadBalancer.setThreadsNumbersMap(threadsNumbersMap);
        this.aliveTmNumber = Collections.frequency(this.aliveTaskManagers.values(), true);
        System.out.println("JobManager: number of alive TaskManagers is " + this.aliveTmNumber);
        List<List<StreamProcessorProperties>> pipelines = createPipelines();
        this.tmProcessors = this.loadBalancer.assignProcessors(pipelines, this.tmProcessors);
        this.sendSerializedPipelines(this.tmProcessors);

        consumer.close();
        producer.close();
    }

    private void sendSerializedPipelines(List<List<StreamProcessorProperties>> processors) {
        JsonPropertiesSerializer serializer = new JsonPropertiesSerializer();
        KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.getProducerProperties());

        System.out.println("JobManager: sending json serialized processors properties");

        for (int i = 0; i < this.tmNumber; i++) {
            if (this.aliveTaskManagers.get(i)) { // if i-th task manager is alive
                List<StreamProcessorProperties> processorsPropertiesForTM = processors.get(i);

                for (StreamProcessorProperties props : processorsPropertiesForTM) {
                    String jsonProperties = serializer.serialize(props);

                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(Config.PROCESSORS_TOPIC + "_" + i, String.valueOf(i), jsonProperties);
                    producer.send(record);
                }

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(Config.PROCESSORS_TOPIC + "_" + i, String.valueOf(i), "eos");
                producer.send(record);
            }
        }

        System.out.println("JobManager: done settings");

        producer.close();
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

    private void createTopics() {
        TopicsManager topicsManager = TopicsManager.getInstance();
        List<String> topics = Parser.parseTopics();
        topicsManager.setSourceTopic(Config.SOURCE_TOPIC);
        topicsManager.setSinkTopic(Config.SINK_TOPIC);
        topicsManager.createTopics(topics);
        topicsManager.createTopics(Collections.singletonList(Config.HEARTBEAT_TOPIC));
        topicsManager.createTopics(Collections.singletonList(Config.HEARTBEAT_EVENTS_TOPIC));
        topicsManager.createTopics(Collections.singletonList(Config.THREADS_TM_TOPIC));
        topicsManager.createTopics(Collections.singletonList(Config.THREADS_JM_TOPIC));
        topicsManager.createTopics(Collections.singletonList(Config.PROCESSORS_TOPIC));
    }
}