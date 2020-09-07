package it.polimi.middleware.kafka_pipeline.common;

public class Config {

    public static String CONFIG_FILE;
    public static String PIPELINE_FILE;

    public static String SERVER_IP;
    public static int SERVER_PORT;

    public static String JOB_NAME;
    public static String PROCESSORS_CONSUMER_GROUP;

    public static int PARALLELISM;
    public static int NUM_TOPICS_PARTITIONS;
    public static short REPLICATION_FACTOR;

    public static int TM_NUMBER;

    public static String SOURCE_TOPIC;
    public static String SINK_TOPIC;
    public static final String SOURCE_KEYWORD = "source";
    public static final String SINK_KEYWORD = "sink";

    public static final String HEARTBEAT_TOPIC = "heartbeat_topic"; // unidirectional TaskManager -> JobManager
    public static final String HEARTBEAT_EVENTS_TOPIC = "heartbeat_events_topic"; // HeartbeatController -> JobManager
    public static final String THREADS_TM_TOPIC = "threads_tm_topic"; // unidirectional TaskManager -> JobManager
    public static final String THREADS_JM_TOPIC = "threads_jm_topic"; // unidirectional JobManager -> TaskManager
    public static final String PROCESSORS_TOPIC = "processors_topic"; // unidirectional JobManager -> TaskManager

    public static void printConfiguration(){
        System.out.println("\n----------   CONFIG   ----------");
        System.out.println("Server ip: " + Config.SERVER_IP);
        System.out.println("Server port: " + Config.SERVER_PORT);
        System.out.println("Task managers number: " + Config.TM_NUMBER);
        System.out.println("Parallelism: " + Config.PARALLELISM);
        System.out.println("Job name: " + Config.JOB_NAME);
        System.out.println("Processors consumer group: " + Config.PROCESSORS_CONSUMER_GROUP);
        //System.out.println("Replication factor: " + Config.REPLICATION_FACTOR);
        //System.out.println("Num topics partitions: " + Config.NUM_TOPICS_PARTITIONS);
        System.out.println("Source topic: " + Config.SOURCE_TOPIC);
        System.out.println("Sink topic: " + Config.SINK_TOPIC);
        System.out.println("--------------------------------\n");
    }

}
