package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;
import it.polimi.middleware.kafka_pipeline.threads.heartbeat.HeartbeatController;

import java.util.ArrayList;
import java.util.List;

public class JobManager {

    private List<TaskManager> taskManagers;
    private HeartbeatController heartbeatController;
    private int tmNumber = 2;

    public JobManager() {
        this.taskManagers = new ArrayList<>();

        List<List<StreamProcessor>> pipelines = createPipelines();
        List<List<StreamProcessor>> tmProcessors = createProcessorsLists();

        tmProcessors = assignProcessors(pipelines, tmProcessors);

        for (int i = 0; i < tmNumber; i++) {
            TaskManager tm = new TaskManager(i, tmProcessors.get(i));
            taskManagers.add(tm);
            tm.createThreads();
        }

        this.heartbeatController = new HeartbeatController(this.taskManagers);
    }

    public void start() {
        for (TaskManager tm : taskManagers) {
            tm.start();
        }

        System.out.println("JobManager : starting heartbeat controller thread");
        heartbeatController.start();
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

    private List<List<StreamProcessor>> createProcessorsLists() {
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