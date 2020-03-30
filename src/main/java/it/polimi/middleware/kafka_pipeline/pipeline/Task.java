package it.polimi.middleware.kafka_pipeline.pipeline;

import it.polimi.middleware.kafka_pipeline.common.Utils;
import it.polimi.middleware.kafka_pipeline.parser.Parser;

/**
 * This class represents the main components of an application.
 *
 * Each class contains a copy of the pipeline.
 */
public class Task {

    private int id;
    private Pipeline pipeline;

    public Task(int id) {
        this.id = id;
        this.pipeline = new Pipeline(Parser.parseProcessorsMap(id, Utils.getProducerProperties(),
                                                                    Utils.getConsumerProperties()));
    }

    public void proceed() {
        pipeline.process();
    }

    public void stop() {
        pipeline.stopPipeline();
    }

    public int getId() { return this.id; }

}
