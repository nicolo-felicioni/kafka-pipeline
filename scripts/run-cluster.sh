#!/bin/bash

KAFKA_PROPERTIES=../config/server.properties
ZOOKEEPER_PROPERTIES=../config/zookeeper.properties

cd ~/kafka_2.12-2.4.0
cd bin

# start zookeeper and kafka
./zookeeper-server-start.sh $ZOOKEEPER_PROPERTIES &
sleep 3
./kafka-server-start.sh $KAFKA_PROPERTIES &

# wait until the user inputs 'stop', then stop both kafka and zookeeper
stop=0
while [ $stop -eq 0 ]
do
    read -e input
    if [ "$input" = "stop" ]; then
        stop=1
        ./kafka-server-stop.sh
        sleep 3
        ./zookeeper-server-stop.sh
    fi
done