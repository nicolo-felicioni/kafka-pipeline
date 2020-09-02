package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PipelineThread extends Thread {

    private String id;
    private List<StreamProcessor> processors;
    private boolean running = false;
    private final Lock lock = new ReentrantLock();

    public PipelineThread(int id, int taskManagerId) {
        this.id = taskManagerId + "." + id;
        this.processors = new ArrayList<>();
    }

    @Override
    public void run() {

        System.out.println("Starting thread " + id + " with processors " + processors);

        running = true;

        while(running) {
            synchronized (lock) {
                for (StreamProcessor p : processors) {
                    System.out.println("Thread " + id + " - Running processor " + p.getId());
                    p.process();
                }
            }
        }
    }

    public String getID() { return this.id; }

    public int getProcessorsNumber() { return processors.size(); }

    public List<StreamProcessor> getProcessors() { return processors; }

    public void assign(StreamProcessor p) {
        synchronized (lock) {
            processors.add(p);
        }
    }

    @Override
    public void interrupt() {
        for(StreamProcessor p : processors)
            p.stop();
        this.running = false;
    }
}
