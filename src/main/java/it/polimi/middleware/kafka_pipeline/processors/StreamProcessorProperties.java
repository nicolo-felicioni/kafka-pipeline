package it.polimi.middleware.kafka_pipeline.processors;

import it.polimi.middleware.kafka_pipeline.topics.TopicsManager;

import java.util.Properties;

public class StreamProcessorProperties {

    private Properties props;

    public StreamProcessorProperties(int taskID, String ID, String type, String from, String to) {
        props = new Properties();
        props.put("taskID", String.valueOf(taskID));
        props.put("ID", ID);
        props.put("type", type);
        props.put("from", from);
        props.put("to", to);

        props.put("input_topic", TopicsManager.getInputTopic(from, ID));
        props.put("output_topic", TopicsManager.getOutputTopic(ID, to));
    }

    public int getTaskID() {
        return Integer.parseInt(props.getProperty("taskID"));
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

    public String getOutputTopic() {
        return props.getProperty("output_topic");
    }

    public void setTaskID(int taskID) {
        props.setProperty("taskID", String.valueOf(taskID));
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
