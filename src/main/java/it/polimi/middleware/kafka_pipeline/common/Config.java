package it.polimi.middleware.kafka_pipeline.common;

public class Config {

    public static String CONFIG_FILE = "config.yaml";
    public static String PIPELINE_FILE = "pipeline.yaml";

    public static String SERVER_IP;
    public static int SERVER_PORT;

    public static String GROUP;

    public static int THREADS_NUM;
    public static short REPLICATION_FACTOR;
    public static int NUM_TOPICS_PARTITIONS;

    public static void printConfiguration(){
        System.out.println("\nServer ip: " + Config.SERVER_IP);
        System.out.println("Server port: " + Config.SERVER_PORT);
        System.out.println("Threads num: " + Config.THREADS_NUM);
        System.out.println("Group: " + Config.GROUP + "\n");
        System.out.println("Replication factor: " + Config.REPLICATION_FACTOR + "\n");
        System.out.println("Num topics partitions: " + Config.NUM_TOPICS_PARTITIONS + "\n");
    }

}
