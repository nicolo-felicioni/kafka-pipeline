package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.common.Config;
import it.polimi.middleware.kafka_pipeline.pipeline.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Class that manages threads execution.
 *
 * It maintains a list of all the tasks
 * and it assigns them to threads.
 */
public class ThreadsExecutor {

    private List<Task> tasks;
    private ExecutorService executor;

    public ThreadsExecutor(List<Task> tasks) {
        this.tasks = tasks;
        executor = Executors.newFixedThreadPool(Config.PARALLELISM);

        createThreads();
    }

    /**
     * Create threads, assign them to tasks and start executing them.
     */
    private void createThreads() {
        int tasksPerThread = Config.TASKS_NUM / Config.PARALLELISM;
        List<PipelineThread> threads = new ArrayList<>();
        System.out.println("Tasks per thread: " + tasksPerThread);
        int id = 0;
        for(int i = 0; i < Config.TASKS_NUM; i += tasksPerThread) {
            List<Task> toBeAssigned = new ArrayList<>();
            PipelineThread currentThread;
            for (int j = i; j < i + tasksPerThread; j++) {
                Task task = tasks.get(j);
                System.out.println("Assigning task: " + task.getId());
                toBeAssigned.add(task);
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

    /**
     * Stop the executor (i.e. stop all the threads)
     */
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
