<p align="center">
  <img height="206" src="https://user-images.githubusercontent.com/16304728/75275484-e4cd6180-5804-11ea-971b-0e3b40bc487e.png">
</p>

# Apache Kafka Data Processing Pipeline 
## Features
- Provide administrative tools / scripts to create and deploy a processing pipeline that processes
messages from a given topic.
- A processing pipeline consists of multiple stages, each of them processing an input message
at a time and producing one output message for the downstream stage.
- Different processing stages could run on different processes for scalability.
- Messages have a key, and the processing of messages with different keys is independent.
- Messages having the same key are processed in FIFO order with end-to-end exactly once
delivery semantics. 

## Team Members
- Mattia Righetti
- Nicolò Felicioni
- Luca Conterio
