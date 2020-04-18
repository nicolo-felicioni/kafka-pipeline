package it.polimi.middleware.kafka_pipeline.processors;

import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;

import java.util.Properties;

public class StreamProcessorProperties {

    private Properties props;

    public StreamProcessorProperties(int pipelineID, String ID, String type, String from, String to) {
        props = new Properties();

        props.put("pipelineID", String.valueOf(pipelineID));
        props.put("ID", ID);
        props.put("type", type);
        props.put("from", from);
        props.put("to", to);

        props.put("input_topic", TopicsManager.getInputTopic(from, ID));
        props.put("output_topic", TopicsManager.getOutputTopic(ID, to));
        props.put("state_topic", TopicsManager.getStateTopic(ID));
    }

    public int getPipelineID() {
        return Integer.parseInt(props.getProperty("pipelineID"));
    }

    public String getID() {
        return props.getProperty("ID");
    }

    public String getType() {
        return props.getProperty("type");
    }

    public String getFrom() {
        return props.getProperty("from");
    }

    public String getTo() {
        return props.getProperty("to");
    }

    public String getInputTopic() {
        return props.getProperty("input_topic");
    }

    public String getStateTopic() {
        return props.getProperty("state_topic");
    }

    public String getOutputTopic() {
        return props.getProperty("output_topic");
    }

    public void setPipelineID(int pipelineID) {
        props.setProperty("pipelineID", String.valueOf(pipelineID));
    }

    public void setID(String ID) {
        props.setProperty("ID", ID);
    }

    public void setType(String type) {
        props.setProperty("type", type);
    }

    public void setFrom(String from) {
        props.setProperty("from", from);
    }

    public void setTo(String to) {
        props.setProperty("to", to);
    }

    public void setInputTopic(String inTopic) {
        props.setProperty("input_topic", inTopic);
    }

    public void setOutputTopic(String outTopic) {
        props.setProperty("output_topic", outTopic);
    }
}
