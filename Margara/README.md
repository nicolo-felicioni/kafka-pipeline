# Kafka tutorial

### Admin (start Kafka and create a topic)

From the Kafka home directory

* Start ZooKeeper: ./bin/zookeeper-server-start.sh config/zookeeper.properties
* Start a Kafka server: ./bin/kafka-server-start.sh config/server.properties
* Create a topic: ./bin/kafka-topics.sh --create --topic topic1 --replication-factor 1 --partitions 3 --zookeeper localhost:2181

### Basic

* Presents the basic producer and consumer API
* Illustrates both automatic and manual commit of offset
* Enables testing the semantics of consumers and consumer groups

### Basic transactions

* Exemplifies the use of transactions and different isolation levels
* TransactionalProducer commits even messages and aborts odd ones
* TransactionalConsumer can be configured to consume all messages or only committed ones 

### Atomic forward

* The AtomicForwarder exploits manual manual commit and transactions to forward each message exactly once

### Bank

* Shows the use of transactions in a specific application context
* Only committed bank operations are processed by the consumer
* Illustrates the interactions between Kafka and external applications
  * It requires a static consumer group, otherwise the information of a given bank account should be migrated  
  * It requires that consumers do not crash (or re-execute all commands from the beginning or from a stable snapshot of their state)
  
### Bank transfer

* Exemplifies a partitioning problem
  * It is not possible to process messages in different partitions without violating the application semantics