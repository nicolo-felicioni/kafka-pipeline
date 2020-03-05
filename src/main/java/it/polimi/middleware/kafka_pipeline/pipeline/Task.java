package it.polimi.middleware.kafka_pipeline.pipeline;

public class Task {

    private int id;
    private Pipeline pipeline;

    public Task(int id, Pipeline pipeline) {
        this.id = id;
        this.pipeline = pipeline;
    }

    public void proceed() {
        pipeline.process();
    }

    public void stop() {
        pipeline.stopPipeline();
    }

    public int getId() { return this.id; }

}
