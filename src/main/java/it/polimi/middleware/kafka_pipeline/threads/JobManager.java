package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.JsonPropertiesSerializer;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessorProperties;
import it.polimi.middleware.kafka_pipeline.threads.heartbeat.HeartbeatController;
import it.polimi.middleware.kafka_pipeline.unused.Pipeline;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

public class JobManager {

    private HeartbeatController heartbeatController;
    private List<List<StreamProcessorProperties>> tmProcessors;
    private int tmNumber;
    private boolean running = false;

    public JobManager() {
        this.tmNumber = Config.TM_NUMBER;

        List<List<StreamProcessorProperties>> pipelines = createPipelines();
        this.tmProcessors = createTMProcessorsLists();

        tmProcessors = assignProcessors(pipelines, tmProcessors);

        System.out.println("JobManager : starting heartbeat controller thread");
        this.heartbeatController = new HeartbeatController(this.tmNumber);
        this.heartbeatController.start();

        this.sendSerializedPipelines(tmProcessors);
    }

    public void start() {
        running = true;

        /*try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.getProducerProperties());

        while(running) {
            //System.out.println("Running ...");

            for (int i = 0; i < tmNumber; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(Config.SETTINGS_TOPIC + "_" + i, String.valueOf(i), "debug");

                producer.send(record);
                System.out.println("Sent debug message");
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
    }

    public void stop() {
        running = false;
    }

    private void sendSerializedPipelines(List<List<StreamProcessorProperties>> processors) {
        JsonPropertiesSerializer serializer = new JsonPropertiesSerializer();
        KafkaProducer<String, String> producer = new KafkaProducer<>(Utils.getProducerProperties());
        for (int i = 0; i < tmNumber; i++) {

            List<StreamProcessorProperties> processorsPropertiesForTM = processors.get(i);

            for (StreamProcessorProperties props : processorsPropertiesForTM) {
                String jsonProperties = serializer.serialize(props);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(Config.SETTINGS_TOPIC + "_" + i, String.valueOf(i), jsonProperties);

                producer.send(record);
            }

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(Config.SETTINGS_TOPIC + "_" + i, String.valueOf(i), "eos");
            producer.send(record);
        }
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

    private List<List<StreamProcessorProperties>> assignProcessors(List<List<StreamProcessorProperties>> pipelines,
                                                         List<List<StreamProcessorProperties>> processors) {
        // round robin assignment of operators to task managers
        int tm_index = 0;
        for(int i = 0; i < pipelines.get(0).size(); i++) {
            for(int j = 0; j < pipelines.size(); j++) {
                StreamProcessorProperties p = pipelines.get(j).get(i);
                System.out.println("JobManager : assigning to TaskManager " + tm_index + " processor " + p);
                processors.get(tm_index).add(p);
                tm_index = (tm_index + 1) % tmNumber;
            }
        }
        return processors;
    }
}