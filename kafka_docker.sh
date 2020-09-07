#!/bin/bash

NUM_BROKER=2
FORCE=1

for arg in "$@"
do
    case $arg in
        -h|--help)
        echo "Options:"
        echo "    -h, --help                show list of options"
        echo "    -d, --default             deploy three kafka brokers"
        echo "    -bn, --broker_number      deploy a specified number of kafka brokers (max 7)"
        echo "    -f, --force               force deploy a specified number of kafka brokers greater than 7"
        exit 0
        ;;
        -d|--default)
        NUM_BROKER=3
        shift
        ;;
        -bn=*|--broker_number=*)
        NUM_BROKER="${arg#*=}"
        shift
        ;;
        -f|--force)
        FORCE=0
        shift
        ;;
        *)
        echo "Invalid argument, run -h to see a list of commands available"
        exit 1
        ;;
    esac
done

if [ $NUM_BROKER -gt 7 ]
then
echo greater than 7
if [ $FORCE -gt 0 ]
then
    echo "You can't run more than 7 brokers, use the -f flag instead."
    exit 0
fi
fi


export DOCKER_HOST_IP=$(ifconfig | awk '/192.168.1./ {print $2}')
docker-compose down
docker-compose up -d
docker-compose scale kafka=$NUM_BROKER
exit 0
