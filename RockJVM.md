<a href="https://medium.com/@lackshub/notes-for-databricks-crt020-exam-prep-9fbc97a2147e"> Databricks Preparation test </a>
# Spark Cluster Manager
Is a series of processes. Each process is started in separated machines.
One of the process is the driver (manage the state of the entire cluster), the other process (worker process) performs the tasks.
Spark supports 3 cluster managers
1. Standalone
2. Yarn
3. Mesos

Client vs Cluster mode: https://stackoverflow.com/questions/41124428/spark-yarn-cluster-vs-client-how-to-choose-which-one-to-use

# Garbage Collector
Allows automatically free up memory space that has been allocated to objects no longer needed by the program.
Garbage collector will need to trace through all your Java objects and find the unused ones, in order to open space to new objects.
The cost of garbage collection is proportional to the number of Java objects
The first thing to try if GC is a problem is to use serialized caching. ```DF.cache``` (but remember that LRU applies, Least Recently Used)
Implementation of GC in Java:

* Java Heap Space is divided in 2 regions: Young and Old
* Young is composed of [Eden, Survivor 1, Survivor 2]
* When Eden is Full, GC objects of Eden and Survivor 1 migrate to Survivor 2.
* If object of Survivor 2 is older enough, or the region if full, migrates to Old
* When Old is closed to full, GC is invoke to release memory.

The goal of GC in Spark is to store long lived RDDs in Old Region, and to store in Young sufficiently sized to store short-lived objects (task intermedia objects).

Check:
* If a full GCs is invoked multiple times while executing task -no sufficient mem to execute task-
* If minor GCs (migrate Young to Old), allocate more resources on Eden.
* If the OldGen is close to being full, reduce the amount of memory used for caching by lowering spark.memory.fraction; it is better to cache fewer objects than to slow down task execution. 

<img src="https://miro.medium.com/max/1400/1*bfaBln8nWqy5dCE4Dtqq0Q.png">

# Memory Usage in Spark
<a href="https://community.cloudera.com/t5/Community-Articles/Spark-Memory-Management/ta-p/317794">More Information</a>
<img src="https://community.cloudera.com/t5/image/serverpage/image-id/31614iEBC942A7C6D4A6A1/image-size/large?v=v2&px=999">
JVM memory can have 2 parts: Heap Space (GC acts) and Off-Heap Space (GC do nothing, objects are store outside the JVM by serializartion).
In spark, memory management is composed of 2 types:

* Static Memory Manager: Divide the memory into equal partitions. Is fixed. Does not support the use of off-heap memory. Deprecated because lack of flexibility. 
* Unified Memory Manager: Replace SMM, to provide spark with dynamic memory allocation. The storage and execution share this memory. If any of the storage or execution memory needs more space, increase one and decrease the other. Spark tasks operate in two main memory regions: <b>Execution</b> – Used for shuffles, joins, sorts and aggregations , <b>Storage</b> – Used to cache partitions of data. 
* Storage UMM: any persist option that includes MEMORY in it, Spark will store that data in this segment, Spark clears space for new cache requests by removing old cached objects based on Least Recently Used (LRU) mechanism, Once the cached data it is out of storage, it is either written to disk or recomputed based on configuration. Broadcast variables are stored in cache with MEMORY_AND_DISK persistent level.
* Execution UMM: For example, it is used to store shuffle intermediate buffer on the Map side in memory. Also, it is used to store hash table for hash aggregation step. Execution memory tends to be more short-lived than storage. It is evicted immediately after each operation, making space for the next ones.

### On Heap Memory
By default, Spark uses on-heap memory only.
<img src="https://community.cloudera.com/t5/image/serverpage/image-id/31457iBFE248CFD18E3363/image-size/large?v=v2&px=999">
<img src="https://russianblogs.com/images/369/1ea881d4ddb7af70c336e7d052394751.png">
<img src="https://miro.medium.com/max/1400/1*5VR7QSyoz1kFuFCuYKqB7w.png">
* <b>Reserve Memory: </b> uses to store spark default objects, and cannot be change (300MB by default).
* <b>User Memory: </b> used to store user-defined data structures, Spark internal metadata, any UDFs created by the user, the data needed for RDD conversion operations such as the information for RDD dependency information etc. This memory segment is not managed by Spark.
* <b>Spark Memory (Unified Memory): </b> Spark Memory is responsible for storing intermediate state while doing task execution like joins or storing the broadcast variables. All the cached/persisted data will be stored in this segment, specifically in the storage memory of this segment.
* Storage and Execution pool borrowing rules: 
    * Storage memory can borrow space from execution memory only if blocks are not used in Execution memory.
    * Execution memory can also borrow space from Storage memory if blocks are not used in Storage memory.
    * If blocks from Execution memory is used by Storage memory, and Execution needs more memory, it can forcefully evict the excess blocks occupied by Storage Memory
    * If blocks from Storage Memory is used by Execution memory and Storage needs more memory, it cannot forcefully evict the excess blocks occupied by Execution Memory; it will end up having less memory area. It will wait until Spark releases the excess blocks stored by Execution memory and then occupies them.

### Off Heap Memory
* It can still reduce memory usage, reduce frequent GC, and improve program performance.
* When an executor is killed, all cached data for that executor would be gone but with off-heap memory, the data would still persist. The lifetime of JVM and lifetime of cached data are decoupled.
* Stores objects in a serialize way.
* Project Tungsten store information in this kind of heap. Tungsten also has the feature of WholeCodeGeneration (spark DAGs). Spark SQL and RDD transformation can be benefit from Tungsten. Wherever WholeCodeGeneration is present in your DAG, it means that Tungsten is active.


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

### Adaptive query execution

<a href="https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html">Link</a>
<img src="https://databricks.com/wp-content/uploads/2020/05/blog-adaptive-query-execution-5.png">
<img src="https://databricks.com/wp-content/uploads/2020/05/blog-adaptive-query-execution-6.png">


### Constant folding
process of recognizing and evaluating constant expressions at compile time rather than computing them at runtime. ```lit(1) + lit(2)``` can be optimize just calling ```lit(3)```

### Schema DDL trick
<a href="https://sparkbyexamples.com/spark/convert-case-class-to-spark-schema/"> Link </a>

### Predicate pushdown
correpond to where clause in sql. 

### Projection Prunning
correspond to select just the columns that will be used in the process. 

### Coalesce vs Repartition, Partitioning
Coalesce is used to decreased the number of partition, when used to increase is esquivalent to ```coalesce(number, true)```, which is the same as repartition.
Coalesce is dont involve shuffle normally (narrow dependency), so it will be in the same stage, and repartition, viceversa (wide dependency).

* Use coalesce when you want to <b>REDUCE</b> the number of partitions, and you dont care how data is distributed
* Use repartition when you want to increase parallelism/number of partitions, or you want to control partition size, or you want to redistribute data evently.

The optimal partition size should be between 10-100MB of uncompress data. You can configure the number of partitions at a shuffle (default is 200, ```spark.sql.shuffle.partitions=100, spark.default.paralellism=100```). 
There are various ways to estimate the size of a dataframe. 
* One way is cache in a mem or disk and see in spark UI how much size occupies (compress data). 
* The second way is make a function. Its better to use cache, size estimator could report a higher number.
```scala
def dfSizeEstimator() = {
       val numbers = spark.range(100000)
       println(org.apache.spark.util.SizeEstimator.estimate(numbers)) //return number of bytes. Gives a larger number because it outputs jvm size object. 
       numbers.cache()
       numbers.count()
}
```
* The third way is to use query plan
```scala
def estimateWithQueryPlan() = {
       val numbers = spark.range(1000)
       println(numbers.queryExecution.optimizedPlan.stats.sizeInBytes) //its worth.
}
```

### Shuffle Partitioning Exercises 
[Link](Shuffle.pdf)
<a href="https://stackoverflow.com/questions/66475883/setting-number-of-shuffle-partitions-per-shuffle-in-the-same-spark-job">Guide</a>

You can shuffle data with repartition before a join to increase the number of partitions. 

### Partitioners RDDs
[Link](Partitioners.pdf)

### How to set up a cluster
[Link](Resources.pdf)

### Persistent Data
Cache and Persist are the same operation (```.persist()``` allows to specify where persist data). For uncaching/unpersist just make ```.unpersist()```. Just cache data that will be used in multiple computations, but remember, there is an investment on it, because caching requires time to write data to mem/disk. Do not cache data that will not fit in memory.  Caching RDDs is more costly than dataframes. 

<a href="https://towardsdatascience.com/best-practices-for-caching-in-spark-sql-b22fb0f02d34">More info</a>
<br>
Note: [Caching](Caching.pdf)

Application

```scala
df1 = Seq.fill(50)(Random.nextInt).toDF("C1")
df2 = d1.withColumn("C2", rand()).join(df1, "C1").cache()
df3 = d1.withColumn("C3", rand()).join(df2, "C2")
df4 = d1.withColumn("C4", rand()).join(df3, "C3")
df5 = d1.withColumn("C5", rand()).join(df4, "C4")
df5.union(df1).count() // The action will read twice df1, so it will be worth save the precompute value.
```

* if a job is slow, use caching
* if a job is failing, use checkpointing. 

### Data Skews and Struggling Tasks
[Link](dataSkewsAndStrugglingTasks.scala)

### Query Planning
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

### Tungsten
Motivation: read and cache information in a efficient way.

<a href="https://www.linkedin.com/pulse/catalyst-tungsten-apache-sparks-speeding-engine-deepak-rajak/">More details</a>


## Physical Plan Terminology:
Plan: Read from bottom to top.
Exchange: it happends when repartition. It could be round robin partition. In the exchange physical plan operation its shown the exhanged column. 
SerializeFromObject: takes a big amount of time. Every single element needs to be evaluated individually. The convertion between rdd and df trigguer this task.
Number of tasks: Number of partitions produced in each stages.
Whole Stage Code Generation: Tungstein code generation.

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


### Paritioning and Bucketing
<b>Partitioning prunning: </b> The scan reads only the directories that match the partition filters, thus reducing disk I/O
<b>Partitioning Tips: </b>You want to avoid too many small files, which make scans less efficient with excessive parallelism. You also want to avoid having too few large files, which can hurt parallelism.

<b>Bucketing</b> is another data organization technique that groups data with the same bucket value across a fixed number of “buckets.”. This can improve performance in wide transformations and joins by avoiding “shuffles.”. Bucketing is similar to partitioning, but partitioning creates a directory for each partition, whereas bucketing distributes data across a fixed number of buckets by a hash on the bucket value. Tables can be bucketed on more than one value and bucketing can be used with or without partitioning. 

Partitioning should only be used with columns that have a limited number of values; bucketing works well when the number of unique values is large.	
When to Use Bucket Columns:
* Table size is big (> 200G).
* The table has high cardinality columns which are frequently used as filtering and/or joining keys.
* The sort merge join (without bucket) is slow due to shuffle not due to data skew

Bucketing and saving is almost expensive as a regular shuffle. 

Data Skew: Same key will remain on the same executor. The only way of solve this is turn data in a more uniform way, add data and join/group by noise data and then get rid of it. 

<a href="https://selectfrom.dev/apache-spark-partitioning-bucketing-3fd350816911">More Information</a>

```scala
object joinPartitionerProblems{
    val initialTable = ???.repartition(10)
    val narrowTable = (???).repartition(7) // many records
    val wideTable = f(initialTable) /// many Columns

    //scenario 1
    val joinSparkPartitioning = wideTable.join(narrowTable, "id")
    /*
    QUERY PLAN
    1. There are 2 shuffles involved, 2 repartitions.
    2. There is 1 projection, that involves the creation of the wide dataframe.

    Project [id, ...]
        SortMergeJoin
            Sort[id]
                Exchange HashPartitioning(id)
                    Project // creating the wide dataframe
                        ExchangeRoundRobinPartitioning(10)
                            ???
            Sort[id]
                Exchange HashPartitioning(id)
                        ExchangeRoundRobinPartitioning(7)
                            ???
    */


    //scenario 2
    val altNarrow = narrowTable.repartition($"id")
    val altInitial = initialTable.repartition($"id")
    val joinPrepartitioning = altInitial.join(altNarrow, "id")
    val result2 = f(joinPrepartitioning) // add many columns
    /*
    QUERY PLAN:
    1. There is only one shuffle stage, and is the hashpartitioning by id. Despite that initialTable is repartition by 10, we are saying that will be repartitioned by id, and because is the same dataframe, it will win the last repartition.
    2. Because both dataframes has the same partition we've 2 co-partitioned dataframes. (HashPartitioner by id).
    Note: It is always a good Idea to Partition Early the dataframes, not late. Because late it will cause performance issues as scenario 3

    Project
        SortMergeJoin
            Sort
                Exchange HashPartitioning [id]
                    ???
            Sort
                Exchange HashParitioning [id]
                    ???
    */

    //scenario 3
    val repartitionedNarrow = narrowTable.repartition($"id")
    val repartitionedWide = wideTable.repartition($"id")
    val joinNotUseful = repartitionedWide.join(repartitionedNarrow, "id")
    /*
    QUERY PLAN
    1. Because we have a projection in the midle, spark firtst must do repartition(10) and after that repartition by id
    2. This kind of approach id the same as not doing repartition by id explicitly, because spark will also do that operation by default, so its not worth to put that in the code. 


    Project
        SortMergeJoin
            Sort
                Exchange HashPartitioner(id, 200)
                    Project
                        ExchangeRoundRobinPartitioning(10)
                            ???
            Sort
                Exchange HashPartitioner(id, 200)
                    ???
    */
}

object bucketing {
  narrowTable
  .write
  .bucketBy(NoBuckets, colBucket1, colBucket2, ???)
  .sortBy(colBucket1)
  .saveAsTable(NameTable)

  val table = spark.table(NameTable)
}
```

### Join Types
* <b>Hash Join: </b> 
* <b>Broadcast Hash Join</b>
* <b>Sort Merge Join</b>
* <b>Shuffle Nested Loop Join</b>
* <b>Sfhuffle Hash Join</b>

### UDF
### UDAF
