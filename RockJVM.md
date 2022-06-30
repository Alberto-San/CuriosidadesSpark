# Spark Cluster Manager
Is a series of processes. Each process is started in separated machines.
One of the process is the driver (manage the state of the entire cluster), the other process (worker process) performs the tasks.
Spark supports 3 cluster managers
1. Standalone
2. Yarn
3. Mesos

# Spark Driver
Process that manages the state of the stages/task of the application, and interface with the cluster manager.
Cluster driver and cluster worker are different than spark driver and woker process. 

# Spark Executors
runs the task assigned by the Spark driver, and report back their state and the results. 1 executor is 1 JVM on 1 physical machine. Each executor can load 1 partition in mem or disk. Each physical node can have 1 or more executors. 

# Executions modes
1. cluster: the spark driver is launched on a worker node. (remember, there is a driver manager process, that is different than the driver of the application). The cluster manager is responsible for Spark Processes.
```
       Cluster Manager Driver
              |
  ----------------------------------
  |               |                |
  Worker        Worker            Worker
  |               |                 |
  Spark Driver    Spark Executor   Spark Ex Process
                   Process
```
3. client: the spark driver is on a client machine. Executors reports state to the client machine driver.
```
client machine-----------------> Cluster Manager Driver
                                            |
                                 -----------------------
                                 |          |          |
                                 WK         WK         WK
                                 |          |          |
                               EX PR      EX PR       EX PR
```
5. local: the entire application runs in the same machine.
6. Example: ```./bin/spark-submit --class part6practical.TestDeployApp --deploy-mode client --master spark://04df43098382:7077 --verbose --supervise /opt/spark-apps/spark-essentials.jar /opt/spark-data/movies.json /opt/spark-data/goodComedies```
7. master variable refers to the cluster manager.Yarn if hadoop is the manager, mesos will be mesos://IP:PORT, Standalone spark://IP:PORT, kubernetes: 	k8s://HOST:PORT || k8s://https://HOST:PORT, local: local || local[k]; k: number of cores


# Concepts
Stages are made of task, and each task is an operation on a partition
Narrow dependencies: partitions that are used to execute individual task in partitions.
Wide dependencies: multiples partitions are needed to give a result (aggregation, join, sort). Involve shuffle (data transfer between Spark executors)

# Query Planning
Catalyst Query Optimizer

1. When you start a SQL job, spark knows dependencies (for this performs "unresolved logical transformation plan", arraging dataframes in some graph)
Catalyst resolves references and expression types of those DF (through "catalog", the action is called "resolving logical plan", and we end with a resolve logical plan).
2. Logical plan are optimized given "optimize logical plan".
3. After that spark generates a series of physical plans (series of computations that nodes will perform), and they will be compared with a "cost model", after that one of those physical plan will be chosen as the selected physical plan, and that is the one that we will see in the console. 
4. After this plan is generated, spark will generate some java/jvm bytecode, so the actual rdd are produce and execute throughout the cluster. 

```
SQL Query----
            |----> (1)-->catalog-->(2)--->(3)-->optimized plan-->(3)-->physical plans-->(4)--->Selected physical plan ----->(5)-->RDDs
DataFrame----
                   |                                                                                                  |
                   ----------------------------------------CATALYST QUERY OPTIMIZER------------------------------------
```
