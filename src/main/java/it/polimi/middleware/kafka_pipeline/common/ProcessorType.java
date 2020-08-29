package it.polimi.middleware.kafka_pipeline.common;

public enum ProcessorType {

        FORWARD("forward"),
        SUM("sum"),
        COUNT("count"),
        AVERAGE("average"),
        UNKNOWN("unknown");

        /**
         * The string corresponding to each processor type.
         */
        private String processorTypeString;

        /**
         * Constructor.
         */
        ProcessorType(String typeString){
            processorTypeString = typeString;
        }

        @Override
        public String toString() {
                return processorTypeString;
        }
}
