# Spark Cluster Manager
Is a series of processes. Each process is started in separated machines.
One of the process is the driver (manage the state of the entire cluster), the other process (worker process) performs the tasks.
Spark supports 3 cluster managers
1. Standalone
2. Yarn
3. Mesos

Client vs Cluster mode: https://stackoverflow.com/questions/41124428/spark-yarn-cluster-vs-client-how-to-choose-which-one-to-use

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

Steps:
1. Analysis: turn the unresolved logical plan (dependencie dataframe tree) into result logical plan using catalog engine (use catalog to find where DataFrames, columns are coming from).
2. Optimizations: the next step is to perform some optimizations in the dependencies tree. Spark can identify some optimizations. 
3. After the Optimizations, catalog will produce a bunch of Physical Plans, those will be compare and one will be choose. 
4. Code generation: generate Scala code from execution plan. 

Note: [How catalyst make the optimizations?](Catalyst.pdf)


## Physical Plan Terminology:
Plan: Read from bottom to top.
Exchange: it happends when repartition. It could be round robin partition. In the exchange physical plan operation its shown the exhanged column. 
SerializeFromObject: takes a big amount of time. Every single element needs to be evaluated individually. The convertion between rdd and df trigguer this task.
Number of tasks: Number of partitions produced in each stages.


```scala
val ds1 = spark.range(1, 1000000)
val ds2 = spark.range(1, 10000000, 2)
val ds3 = ds1.repartition(7)
val ds4 = ds2.repartition(9)
val ds5 = ds3.selectExpr("id*3 as id")
val joined = ds5.join(ds4, "id")
val sum = joined.selectExpr("sum(id)")
sum.explain()
>>
(7) HashAggregate(keys=[], functions=[sum(id#18L)])
       Exchange SinglePartition, true, [id=#99]
              (6) HashAggregate(keys=[], functions=[partial_sum(id#18L)])
                     (6) Project[id#18L] // SELECTED COLUMN, in this case, selects right id column
                            (6) SortMergeJoin [id#18L], [id#12L], Inner
                                   (3) Sort [id#18L ASC NULL FIRST], false, 0
                                          Exchange hashpartitioning(id#18L, 200), true, [id=#83]
                                                 (2) Project[(id#10L * 3) AS id#18L]
                                                        Exchange RoundRobinParitioning(7), false, [id=#79]
                                                               (1) Range(1, 1000000, step=1, splits=6)
                                   (5) Sort [id#12L ASC NULL FIRST], false, 0
                                          Exchange hashpartitioning(id#12L, 200), true, [id=#90]
                                                 Exchange RoundRobinPartitioning(9), false, [id=#89]
                                                        (4) Range (1, 10000000, step=2, splits=6)
```

## RDDs vs DATAFRAMES vs Datasets

```scala
val rdd = sc.parallelize(1, 1000000000)
rdd.count
import spark.implicits._
val df = rdd.toDF("id") // performance heat
df.count
df.selectExpr("count(*)").show() //count(*) and count() it should took the same amount of time
val ds = spark.range(1, 1000000000)
ds.count // faster operations than df. The amount of data shuffle is fast, because the transformation between rdd to df trigguer serializefromobject task. 
ds.selectExpr("count(*)").show() // faster operation than df
df.explain
>>Physical plan
       (1) Project
              (1) SerializeFrombject ....
              ...
 val rddTimes5 = rdd.map(_*5)
 rddTimes5.count
 val dfTimes5 = df.selectExpr("id * 5 as id")
 dfTimes5.count // takes almost the same time as rddTimes5
 dfTimes5.explain
 >> Physical Plan
       (1) Project [(value * 5) as id]
              (1) SerializeFromObject [input[0, int, false] as value]
                     Scan[obj]
val dfTimes5Count = dfTimes5.selectExpr("count(*)")
dfTimes5Count.explain // the same that df.count, its the same amount of time, because the multiplication by 5 does matter when executing counting.
val dsTimes5 = ds.map(_*5)
val dsTimes5Count = dsTimes5.selectExpr("count(*)")
val dsCount = ds.selectExpr("count(*)")
dsCount.explain
>> Physical Plan
(2) HashAggregate(keys=[], functions=[count(1)])
       Exchange SinglePartition, true, [id=#253]
              (1) HashAggregate (keys=[], functions=[partial_count(1)])
                     (1) Project
                            (1) Range(1, 1000000000, step=1, splits=6)
 dsTimes5Count.explain
 >> Physical Plan
 (2) HashAggregate(keys=[], functions=[count(1)])
       Exchange SinglePartition, true, [id=#253]
              (1) HashAggregate (keys=[], functions=[partial_count(1)])
                     (1) SerializeFromObject
                            (1) MapElements ....
                                   (1) DeserializeToObject staticinvoke 
                                          (1) Range(1, 1000000000, step=1, splits=6)
 // The execution plans are different, because in case of datasets, lambda functions cannot be optimized, and be ignore, even if the result of tha lambda mapping is not used.
 ```
 
 * Do not convert between APIs levels
 * use DataFrames most of the Time
 * Kryptonite of Datasets is the lambda optimizations that cannot be achieve. 
