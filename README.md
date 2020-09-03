<p align="center">
  <img height="206" src="https://user-images.githubusercontent.com/16304728/75275484-e4cd6180-5804-11ea-971b-0e3b40bc487e.png">
</p>

# Apache Kafka Data Processing Pipeline 
## Assignment
- Provide administrative tools / scripts to create and deploy a processing pipeline that processes
messages from a given topic.
- A processing pipeline consists of multiple stages, each of them processing an input message
at a time and producing one output message for the downstream stage.
- Different processing stages could run on different processes for scalability.
- Messages have a key, and the processing of messages with different keys is independent.
- Messages having the same key are processed in FIFO order with end-to-end exactly once
delivery semantics. 

## Team Members
- [Mattia Righetti](https://github.com/MattRighetti)
- [Nicolò Felicioni](https://github.com/ciamir51)
- [Luca Conterio](https://github.com/luca-conterio)

## Pipeline
In order to describe the structure of the processing pipeline a metalanguage based on `YAML` has to be used.  
The pipeline structure can be defined in `src/main/resources/pipeline.yaml` file.  
  
The `pipeline.yaml` file is a list of processors objects that contains the following fields:
  - `id`: the processor's identifier.
  - `type`: the type of the processor, that can be `forward`, `sum`, `count` or `average` (**TODO**).
  - `to`: the list of processors the current one is connected to (i.e. each processor will send results to the processors contained in this list).
  
The processor that has no incoming arc, will directly be connected to the pipeline source.  
The `sink` keyword is used to indentify the last processor of the pipeline.  
  
Another configuration file is the `config.yaml` that can be found at `src/main/resources` as well. It defines some configuration variables for the system, such as the broker's ip address and port, the number of task managers to be used and the parallelism. In particular this last value indicates the number of replicas the pipeline has (i.e. if parallelism is equal to `n`, then each processor is replicated `n` times).  
In `config.yaml` it is also possible to change the name of the topics that are used as input and output for the pipeline (namely, `source_topic` and `sink_topic`).

### Processors
The available processors types are:
  - `forward`: stateless processor that simply forwards the incoming messages to the processors it is connected to.
  - `sum`: stateless processor that adds a constant number to the value contained in the messages it receives.
  - `count`: stateful processor that counts the occurrenciens of messages with the same key.
  - `average`: stateful processor that computes the average value received in a given time window. **TODO**
  
### Topics
Kafka topics are used to model the arcs of the pipeline graph. In particular the name of each topic is composed by the concatenation of the two extreme processors. For example the arc that goes from processors `p1` and `p2` will be a topic having name `p1_p2`.  
Since data can flow in a single direction in the pipeline, these topics are used as unidirectional communication media: processor `p1` produces messages on topic `p1_p2`, while processor `p2` consumes messages from that same topic.

### Example
```yaml
# SOURCE PROCESSOR
- id: "source_id"
  type: "forward"
  to: ["nodo_1", "nodo_2"]

# INTERMEDIATE PROCESSORS
- id: "nodo_1"
  type: "sum"
  to: ["sink_id"]

- id: "nodo_2"
  type: "count"
  to: ["sink_id"]

# SINK NODE
- id: "sink_id"
  type: "forward"
  to: ["sink"]
```

The example above gives as a result the following pipeline:
<p align="center">
  <img height="200" src="images/pipeline_example.png">
</p>

## Architecture
The main components are:
  - `JobManager`: it acts as a sort of orhcestrator, assigning processors to task managers. It also has a thread called `HeartbeatController` which receives heartbeats from task managers and notifies the job manager whenever one of them crashes. The `LoadBalancer` is used to assign processors to task managers by considering also the number of their available threads and to re-assign processors in the case that some task manager becomes inactive.
  - `TaskManager`: each task manager runs in a different process and possibily on a different machine. They have a set of available `PipelineThread` obejcts and a set of assigned processors.
  - `PipelineThread`: each pipeline thread receives from its parent task manager a set of processors to be executed. Periodically the thread loops over the processors list and execute them.
  - `StreamProcessor`: they are the actual processors of the pipeline. When executed they consume messages from incoming topics, perform some operation (according to their type), save their state (if stateful) and produce the results on outgoing topics.
  - `Kafka Broker`: it manages the topics that are used for each type of communication among components.
  
<p align="center">
  <img height="400" src="images/acrhitecture.png">
</p>

## Pipeline Setup
At startup the job manager sends a start of sentence message ("sos") to task managers, which in turn answer by sending a message that has the key equal to the task manager's identifier and the value equal to the number of task manager's available threads. Through the `LoadBalancer` the processors specified in `pipeline.yaml` are built and assigned to task managers: the job manager sends to each task manager its corresponding list of serialized processors. The coomunication takes place on a different topic for each task manager. The receiver can build its own set of processors and assign them to its threads, which in turn can start the execution.
  
Each task manager main thread is continuously listening for incoming messages from the job manager in the eventuality that a rebalancing of the processors assignment has to take place.

## Rebalancing
Periodically each task manager sends a heartbeat to a specified topic, that is then checked by the job manager via its `HeartbeatController` thread. Whenever the heartbeat is not received during a predefined amount of the, a counter corresponding to the specific task manager is incremented. If the heartbeat is not received for three consecutive periods, the counter reaches 3 and the job manager assumes that the corresponding task manager is no more available. In that case a rebalancing of the processors takes place. In order to re-assing processors, the job manager relies on the `LoadBalancer` component.  
  
Assume to have two task managers with IDs `0` and `1` and that `0` at some point crashes:
  - The job manager notices that the heartbeat of task manager `0` is not received for 3 consecutive periods and assumes it is no more active.
  - Task manager `0` processors are redistributed among the alive task managers and sent to them, which in turn are always listening for messages coming from the job manager.
  - When receiveing “new“ processors, task manager `1` instantiates and distributes them in a round-robin fashion to its threads, which continue with the execution considering also the newly added processors.
