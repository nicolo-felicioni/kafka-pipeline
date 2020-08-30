package it.polimi.middleware.kafka_pipeline.common;

public class TaskManagerIsDownException extends RuntimeException {

    private int taskManagerID;

    public TaskManagerIsDownException(int taskManagerID) {
        super("Task manager " + taskManagerID + " is down");
        this.taskManagerID = taskManagerID;
    }

    public int getTaskManagerDownID() {
        return this.taskManagerID;
    }
}
