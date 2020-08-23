package it.polimi.middleware.kafka_pipeline.processors;

import it.polimi.middleware.kafka_pipeline.common.ProcessorType;
import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;

import java.util.ArrayList;
import java.util.List;

public class StreamProcessorProperties {

    private int pipelineID;
    private String ID;
    private ProcessorType type;
    private List<String> from;
    private List<String> to;
    private List<String> inTopics;
    private List<String> outTopics;
    private String stateTopic;

    public StreamProcessorProperties(int pipelineID, String ID, ProcessorType type) {
        this.pipelineID = pipelineID;
        this.ID = ID;
        this.type = type;

        this.from = new ArrayList<>();
        this.to = new ArrayList<>();

        this.inTopics = new ArrayList<>();
        this.outTopics = new ArrayList<>();

        this.stateTopic = TopicsManager.getStateTopic(this.ID);
    }

    public int getPipelineID() {
        return pipelineID;
    }

    public String getID() {
        return ID;
    }

    public ProcessorType getType() {
        return type;
    }

    public List<String> getFrom() {
        return from;
    }

    public List<String> getTo() {
        return to;
    }

    public List<String> getInTopics() {
        return inTopics;
    }

    public List<String> getOutTopics() {
        return outTopics;
    }

    public String getStateTopic() {
        return stateTopic;
    }

    public void addInput(String f) {
        if (!this.from.contains(f)) {
            this.from.add(f);
            String topic = TopicsManager.getInputTopic(this.ID, f);
            this.inTopics.add(topic);
            System.out.println("Processor " + getID() + ": added input topic " + topic);
        }
    }

    public void addOutput(String t) {
        if (!this.to.contains(t)) {
            this.to.add(t);
            String topic = TopicsManager.getOutputTopic(this.ID, t);
            this.outTopics.add(topic);
            System.out.println("Processor " + getID() + ": added output topic " + topic);
        }
    }

    public String toString() {
        return "Properties - PipelineID: " + getPipelineID() + " - ID: " + getID() + " - Type: " + getType() + " - InTopics: " + getInTopics() + " - OutTopics: " + getOutTopics() + " - State topic: " + getStateTopic();
    }
}
