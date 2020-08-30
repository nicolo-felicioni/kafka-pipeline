package it.polimi.middleware.kafka_pipeline.common;

public class Config {

    public static final String CONFIG_FILE = "config.yaml";
    public static final String PIPELINE_FILE = "pipeline.yaml";

    public static String SERVER_IP;
    public static int SERVER_PORT;

    public static String GROUP;

    public static int PARALLELISM;
    public static short REPLICATION_FACTOR;
    public static int NUM_TOPICS_PARTITIONS;

    public static int TM_NUMBER;

    public static String SOURCE_TOPIC;
    public static String SINK_TOPIC;
    public static final String SOURCE_KEYWORD = "source";
    public static final String SINK_KEYWORD = "sink";

    public static final String HEARTBEAT_TOPIC = "heartbeat_topic";
    public static final String SETTING_THREADS_TOPIC = "settings_threads_topic";
    public static final String SETTINGS_TOPIC = "settings_topic";

    public static void printConfiguration(){
        System.out.println("\nServer ip: " + Config.SERVER_IP);
        System.out.println("Server port: " + Config.SERVER_PORT);
        System.out.println("Task managers number: " + Config.TM_NUMBER);
        System.out.println("Parallelism: " + Config.PARALLELISM);
        System.out.println("Group: " + Config.GROUP);
        System.out.println("Replication factor: " + Config.REPLICATION_FACTOR);
        System.out.println("Num topics partitions: " + Config.NUM_TOPICS_PARTITIONS);
        System.out.println("Source topic: " + Config.SOURCE_TOPIC);
        System.out.println("Sink topic: " + Config.SINK_TOPIC + "\n");
    }

}
