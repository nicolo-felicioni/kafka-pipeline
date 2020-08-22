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

    public static final String HEARTBEAT_TOPIC = "heartbeat_topic";
    public static final String SETTINGS_TOPIC = "settings_topic";

    public static void printConfiguration(){
        System.out.println("\nServer ip: " + Config.SERVER_IP);
        System.out.println("Server port: " + Config.SERVER_PORT);
        System.out.println("Parallelism: " + Config.PARALLELISM);
        System.out.println("Group: " + Config.GROUP);
        System.out.println("Replication factor: " + Config.REPLICATION_FACTOR);
        System.out.println("Num topics partitions: " + Config.NUM_TOPICS_PARTITIONS + "\n");
    }

}
