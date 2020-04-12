package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.parser.Parser;
import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;

import java.util.ArrayList;
import java.util.List;

public class JobManager {

    private List<TaskManager> taskManagers;

    public JobManager() {
        this.taskManagers = new ArrayList<>();

        int tm_num = 2;
        for (int i = 0; i < tm_num; i++)
            taskManagers.add(new TaskManager(i, Config.THREADS_NUM));

        List<StreamProcessor> pipeline = Parser.parsePipeline(Utils.getProducerProperties(),
                                                                            Utils.getConsumerProperties());

        // round robin assignment of operators to taskmanagers
        int tm_index = 0;
        for(int i = 0; i < pipeline.size(); i++) {
            StreamProcessor p = pipeline.get(i);

            System.out.println("JobManager : assigning to TaskManager " + tm_index + " processor " + p);

            taskManagers.get(tm_index).assign(p);

            tm_index = (tm_index + 1) % tm_num;
        }

        for (int i = 0; i < tm_num; i++)
            taskManagers.get(i).createThreads();

        /*for (int i = 0; i < 2; i++){
            List<StreamProcessor> pipeline = Parser.parsePipeline(i, Utils.getProducerProperties(),
                                                                            Utils.getConsumerProperties());
            TaskManager tm = new TaskManager(i, pipeline, Config.THREADS_NUM);
            tm.createThreads();

            taskManagers.add(tm);
        }*/
    }

    public void start() {
        for (TaskManager tm : taskManagers)
            tm.start();
    }




}
