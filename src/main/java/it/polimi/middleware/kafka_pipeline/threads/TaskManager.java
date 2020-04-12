package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;

import java.util.ArrayList;
import java.util.List;

public class TaskManager {

    private int id;
    private int threads_num;
    private List<StreamProcessor> processors;
    private List<PipelineThread> threads;
    //private ExecutorService executor;

    public TaskManager(int id, int threads_num) {   //, List<StreamProcessor> processors) {
        this.id = id;
        this.threads_num = threads_num;
        this.processors = new ArrayList<>();  //processors;
        this.threads = new ArrayList<>();
        //executor = Executors.newFixedThreadPool(threads_num);
    }

    /**
     * Create threads, assign them to tasks and start executing them.
     */
    public void createThreads() {
        for (int i = 0; i < threads_num; i++)
            threads.add(new PipelineThread(i, this.id));

        // round robin assignment of operators to threads
        int thread_index = 0;
        for(int i = 0; i < processors.size(); i++) {
            StreamProcessor p = processors.get(i);

            System.out.println("TaskManager " + id + " : assigning to Thread " + thread_index + " processor " + p);

            threads.get(thread_index).assign(p);

            thread_index = (thread_index + 1) % threads_num;
        }
    }

    public void assign(StreamProcessor p) {
        this.processors.add(p);
    }

    public void start() {
        for (PipelineThread t : threads)
            t.start();
    }

    /**
     * Stop the executor (i.e. stop all the threads)
     */
    public void stop() {
        /*executor.shutdown();
        try {
            while (true) {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS))
                    break;
            }
            System.out.println("Shutdown completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        for (PipelineThread t : threads)
            t.interrupt();

    }

    public int getId() {
        return this.id;
    }


}
