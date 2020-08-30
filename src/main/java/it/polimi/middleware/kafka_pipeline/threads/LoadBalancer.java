package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.processors.StreamProcessorProperties;
import java.util.*;

public class LoadBalancer {

    private Map<Integer, Integer> threadsNumbersMap;

    public LoadBalancer() {}

    public void setThreadsNumbersMap(Map<Integer, Integer> threadsNumbersMap) {
        this.threadsNumbersMap = threadsNumbersMap;
    }

    public int getTMThreadsNumber(int taskManagerID) {
        return this.threadsNumbersMap.get(taskManagerID);
    }

    public List<List<StreamProcessorProperties>> assignProcessors(List<List<StreamProcessorProperties>> pipelines,
                                                                    List<List<StreamProcessorProperties>> assignedProcessors)  {
        // round robin assignment of operators to task managers
        /*int tm_index = 0;
        for(int i = 0; i < pipelines.get(0).size(); i++) {
            for(int j = 0; j < pipelines.size(); j++) {
                StreamProcessorProperties p = pipelines.get(j).get(i);
                System.out.println("JobManager : assigning to TaskManager " + tm_index + " processor " + p);
                processors.get(tm_index).add(p);
                tm_index = (tm_index + 1) % tmNumber;
            }
        }
        return processors;*/

        // build single list with all the processors
        List<StreamProcessorProperties> allProcessorsList = new ArrayList<>();
        // loop on pipelines "by rows"
        for(int i = 0; i < pipelines.get(0).size(); i++) {
            for (int j = 0; j < pipelines.size(); j++) {
                allProcessorsList.add(pipelines.get(j).get(i));
            }
        }

        //System.out.println(allProcessorsList);

        int tm_index = 0;
        int curr_index = 0;
        int curr_threads_number;
        int tmNumber = threadsNumbersMap.size();
        //boolean done = false;

        while(curr_index < allProcessorsList.size()) {

            curr_threads_number = threadsNumbersMap.get(tm_index);

            //System.out.println("curr_index: " + curr_index);
            //System.out.println("curr_threads_number: " + curr_threads_number);

            for(int i = 0; i < curr_threads_number && curr_index+i < allProcessorsList.size(); i++) {
                //System.out.println("   curr_index + i: " + curr_index + i);
                StreamProcessorProperties p = allProcessorsList.get(curr_index + i);
                System.out.println("LoadBalancer : assigning to TaskManager " + tm_index + " processor " + p);
                assignedProcessors.get(tm_index).add(p);
            }

            curr_index += curr_threads_number;
            tm_index = (tm_index + 1) % tmNumber;
        }

        return assignedProcessors;
    }

    public List<List<StreamProcessorProperties>> rebalanceProcessors(int taskManagerDownID, int aliveTmNumber,
                                                                     List<List<StreamProcessorProperties>> processors) {
        List<StreamProcessorProperties> downTaskManagerProcessors = processors.get(taskManagerDownID);
        List<List<StreamProcessorProperties>> reassignedProcessors = new ArrayList<>();

        // round robin re-assignment of operators to TaskManagers
        int tm_index = 0;
        for (int i = 0; i < downTaskManagerProcessors.size(); i++) {
            if (tm_index != taskManagerDownID) {
                StreamProcessorProperties p = downTaskManagerProcessors.get(i);
                // create copy of the i-th processor and assign to TaskManager tm_index
                processors.get(tm_index).add(new StreamProcessorProperties(p.getPipelineID(), p.getID(), p.getType()));
            }
            tm_index = (tm_index + 1) % aliveTmNumber;
        }

        for (int i = 0; i < downTaskManagerProcessors.size(); i++) {
            downTaskManagerProcessors.remove(0);
        }

        // TODO : maybe better to create a new list of lists and send only "new" processors to alive task managers

        return reassignedProcessors;
    }

    /*
    private List<List<StreamProcessorProperties>> reassignProcessors(int taskManagerDownID) {
        List<StreamProcessorProperties> downTaskManagerProcessors = this.tmProcessors.get(taskManagerDownID);
        List<List<StreamProcessorProperties>> reassignedProcessors = new ArrayList<>();
        for (int i = 0; i < this.aliveTmNumber; i++) {
            List<StreamProcessorProperties> p = new ArrayList<>();
            reassignedProcessors.add(p);
        }

        return reassignedProcessors;
    }
     */
}