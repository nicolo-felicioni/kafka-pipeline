package it.polimi.middleware.kafka_pipeline.threads;

import it.polimi.middleware.kafka_pipeline.processors.StreamProcessor;

import java.util.ArrayList;
import java.util.List;

public class PipelineThread extends Thread {

    private String id;
    private List<StreamProcessor> processors;
    private Boolean running;

    public PipelineThread(int id, int taskManagerId) {
        this.id = taskManagerId + "." + id;
        this.processors = new ArrayList<>();
    }

    @Override
    public void run() {

        System.out.println("Starting thread " + id + " with processors " + processors);

        running = true;

        while(running) {
            for(StreamProcessor p : processors) {
                System.out.println("Thread " + id + " - Running processor " + p.getId());
                p.process();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public String getID() { return this.id; }

    public int getProcessorsNumber() { return processors.size(); }

    public void assign(StreamProcessor p) {
        processors.add(p);
    }

    @Override
    public void interrupt() {
        for(StreamProcessor p : processors)
            p.stop();

        this.running = false;
        //this.interrupt();
    }
}
