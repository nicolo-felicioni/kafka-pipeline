package it.polimi.middleware.kafka_pipeline.common;

public class Config {

    public static String CONFIG_FILE = "config.yaml";
    public static String PIPELINE_FILE = "pipeline.yaml";

    public static String SERVER_IP;
    public static int SERVER_PORT;

    public static String GROUP;

    public static int PARALLELISM;
    public static int TASKS_NUM;

    public static void printConfiguration(){
        System.out.println("\nServer ip: " + Config.SERVER_IP);
        System.out.println("Server port: " + Config.SERVER_PORT);
        System.out.println("Tasks num: " + Config.TASKS_NUM);
        System.out.println("Parallelism: " + Config.PARALLELISM);
        System.out.println("Group: " + Config.GROUP + "\n");
    }

}
