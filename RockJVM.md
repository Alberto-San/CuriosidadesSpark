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
runs the task assigned by the Spark driver, and report back their state and the results. 

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
