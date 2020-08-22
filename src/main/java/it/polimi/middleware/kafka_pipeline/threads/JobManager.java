package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.JsonPropertiesSerializer;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.threads.heartbeat.HeartbeatController;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

public class JobManager {

    private HeartbeatController heartbeatController;
    private List<List<StreamProcessor>> tmProcessors;
    private int tmNumber = 1;

    public JobManager() {

        List<List<StreamProcessor>> pipelines = createPipelines();
        this.tmProcessors = createTMProcessorsLists();

        tmProcessors = assignProcessors(pipelines, tmProcessors);

        System.out.println("JobManager : starting heartbeat controller thread");
        this.heartbeatController = new HeartbeatController(this.tmNumber);
        this.heartbeatController.start();

        this.sendSerializedPipelines(tmProcessors);
    }

    public void start() {

    }

    private void sendSerializedPipelines(List<List<StreamProcessor>> processors) {
        JsonPropertiesSerializer serializer = new JsonPropertiesSerializer();
        KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.getProducerProperties());
        for (int i = 0; i < tmNumber; i++) {

            List<StreamProcessor> processorsForTM = processors.get(i);

            for (StreamProcessor processor : processorsForTM) {
                String jsonProperties = serializer.serialize(processor.getProperties());

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(Config.SETTINGS_TOPIC + "_" + i, String.valueOf(i), jsonProperties);

                producer.send(record);
            }

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(Config.SETTINGS_TOPIC + "_" + i, String.valueOf(i), "stop");
            producer.send(record);
        }
    }

    private List<List<StreamProcessor>> createPipelines() {
        // create some pipelines, according to the PARALLELISM parameter
        List<List<StreamProcessor>> pipelines = new ArrayList<>();
        for (int i = 0; i < Config.PARALLELISM; i++){
            List<StreamProcessor> p = Parser.parsePipeline(i);
            pipelines.add(p);
        }
        return pipelines;
    }

    private List<List<StreamProcessor>> createTMProcessorsLists() {
        // create a list of processors for each task manager
        List<List<StreamProcessor>> processors = new ArrayList<>();
        for (int i = 0; i < this.tmNumber; i++) {
            processors.add(new ArrayList<>());
        }
        return processors;
    }

    private List<List<StreamProcessor>> assignProcessors(List<List<StreamProcessor>> pipelines,
                                                         List<List<StreamProcessor>> processors) {
        // round robin assignment of operators to task managers
        int tm_index = 0;
        for(int i = 0; i < pipelines.get(0).size(); i++) {
            for(int j = 0; j < pipelines.size(); j++) {
                StreamProcessor p = pipelines.get(j).get(i);
                System.out.println("JobManager : assigning to TaskManager " + tm_index + " processor " + p);
                processors.get(tm_index).add(p);
                tm_index = (tm_index + 1) % tmNumber;
            }
        }
        return processors;
    }
}