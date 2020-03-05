package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.pipeline.Task;

import java.util.List;

public class PipelineThread extends Thread {

    private int id;
    private List<Task> tasks;
    private Boolean running;

    public PipelineThread(int id, List<Task> tasks) {
        this.id = id;
        this.tasks = tasks;
    }

    @Override
    public void run() {
        running = true;
        while(running) {
            for(Task task : tasks) {
                System.out.println("Thread " + id + " - running task " + task.getId());
                task.proceed();
            }
        }
    }

    public long getId() { return this.id; }

    @Override
    public void interrupt() {
        for(Task task : tasks)
            task.stop();
        this.interrupt();
    }
}
