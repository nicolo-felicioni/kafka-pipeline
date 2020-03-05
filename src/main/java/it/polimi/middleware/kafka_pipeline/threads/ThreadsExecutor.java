package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.pipeline.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *  Maintain a list of all the tasks
 *  and assign them to threads.
 */
public class ThreadsExecutor {

    private List<Task> tasks;
    private ExecutorService executor;

    public ThreadsExecutor(List<Task> tasks) {
        this.tasks = tasks;
        executor = Executors.newFixedThreadPool(Config.PARALLELISM);

        createThreads();
    }

    private void createThreads() {
        int tasksPerThread = Config.TASKS_NUM / Config.PARALLELISM;
        List<PipelineThread> threads = new ArrayList<>();
        System.out.println("Tasks per thread: " + tasksPerThread);
        int id = 0;
        for(int i = 0; i < Config.TASKS_NUM; i += tasksPerThread) {
            List<Task> toBeAssigned = new ArrayList<>();
            PipelineThread currentThread;
            for (int j = i; j < i + tasksPerThread; j++) {
                toBeAssigned.add(tasks.get(j));
                currentThread = new PipelineThread(id, toBeAssigned);
                threads.add(currentThread);
            }
            System.out.println("Created thread with id " + id);
            id++;
        }
        for (PipelineThread t : threads)
            executor.execute(t);

        System.out.println("Threads number: " + threads.size());
    }

    public void shutdown() {
        executor.shutdown();
        try {
            while (true) {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS))
                    break;
            }
            System.out.println("Shutdown completed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
