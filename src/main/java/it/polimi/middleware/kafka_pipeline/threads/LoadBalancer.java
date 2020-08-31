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

        int tm_index = 0;
        int curr_index = 0;
        int curr_threads_number;
        int tmNumber = threadsNumbersMap.size();

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

    public List<List<StreamProcessorProperties>> rebalanceProcessors(int taskManagerDownID,
                                                                     Map<Integer,Boolean> aliveTaskManagers,
                                                                     int tmNumber,
                                                                     List<List<StreamProcessorProperties>> processors) {

        List<StreamProcessorProperties> downTaskManagerProcessors = processors.get(taskManagerDownID);
        List<List<StreamProcessorProperties>> toBeSentProcessors = this.createTMProcessorsLists(tmNumber);

        // round robin re-assignment of operators to TaskManagers
        int tm_index = 0;
        int i = 0;
        int toBeRemovedNumber = downTaskManagerProcessors.size();
        while (i < toBeRemovedNumber) {
            if (aliveTaskManagers.get(tm_index)) { // if TaskManager with id == tm_index is alive
                StreamProcessorProperties p = downTaskManagerProcessors.remove(0);
                // create copy of the i-th processor and assign to TaskManager tm_index
                // also create a new list of lists containing only the objects to be sent to alive TaskManagers
                processors.get(tm_index).add(p.clone());
                toBeSentProcessors.get(tm_index).add(p.clone());
                // move to the next processor (only if allocated to another task manager)
                i++;
            }

            // move to the next task manager
            tm_index = (tm_index + 1) % tmNumber;
        }

        return toBeSentProcessors;
    }

    public List<List<StreamProcessorProperties>> createTMProcessorsLists(int num) {
        // create a list of processors for each task manager
        List<List<StreamProcessorProperties>> processors = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            processors.add(new ArrayList<>());
        }
        return processors;
    }
}