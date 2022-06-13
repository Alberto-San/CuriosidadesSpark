# Table of contents
1. [Basics](#basics)
2. [What is a STAGE?](#stage)
3. [What is a TASK?] (#task)
4. [What is a JOB?] (#job)
5. [Example] (#example)
6. [Transformations] (#transformations)
7. [Actions] (#actions)
8. [Driver and Executors](#driver)
9. [Spark Architecture](#architecture)
10. [Spark Optimizations] (#optimizations)
11. [Spark APIs](#apis)

# Basics <a name="basics"></a>
Think on a DF as a high level API, that is builded on top of RDDs.
RDDs have partitions, and in each partition, when we do map operations, we are asking to do operations in each of the partitions.
Those operations are known as task. When we have a map operation, we are executing a task
```
df = df.selectExpr("(columA + columB) AS sum")
rdd = rdd.map(x => x.split(","))
```
A set of task are known as Stage. But when we have a reduce operation, we need to shuffle the information
```( [a,b,c,a,c,d] => [(a,a), (b), (c,c), (d)] )``` in other to apply reduce operation. That is when the stage is broken, when the shuffle occurs.
This is commonly done, when whatever aggregate operation happends.

<b>Shuffle: </b> Shuffle is important, normally is recommended to have 128MB multiple partitions in other to have a good computational load, 
less shuffle means bigger partitions, and viceversa. Shuffle implies Disk IO, serialization/deserialization, Network IO. Shuffle is not good or bad, normally
the workflow will decide the conditions of the problem. An example of a stage

```
df = df.groupBy(key1).agg(sum(key2))
rdd = rdd.map(x => (x, 1))
rdd = rdd.reduceByKey(_ + _)
```

A set of Stages are known as Jobs. But when an action happens, a job is broken. For example, write actions, checkout, repartition, coalesce actions.
```
df.write.format(...).save(...)
```

# Driver and Executors <a name="driver"></a>
This process as a centralized metadata and the boss aplication, that orders spark executors (slave) to run some amount of work. Driver is responsible for
```
maintaining information about the Spark Application; responding to a user’s program or input;
and analyzing, distributing, and scheduling work across the executors
```
Executors are responsible to run the work assigned, and 
```
reporting the state of the computation on that executor back to the driver node
```

When the driver receives a program:
```
val myRange = spark.range(1000).toDF("number")
```
It will distribute that load into the multiples executors, which will locaye

<b>Modes</b>
<ul>
  <li>
    <b>Cluster Mode: </b> Spark runs its jobs on different machines on different processes
  </li>
  <li>
    <b>Local Mode: </b> Same machine but different processes.
  </li>
</ul>

<b>Note: </b> despite it exists many APIs (Java, Scala, Python, R, ...), the spark code its translate into JVM code, and each executor, run JVM parts of the code. SparkSession is the entrypoint for that, receives Java, Scala, Python, R code, and translate that into JVM instructions that runs in each executor having a cluster architecture.

# Spark Architecture<a name="architecture"></a>
# Spark Submit
Spark submit command is used to lauch an application. It looks like:
```shell
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
```
Where 
* ```class```: entrypoint for your application (main class/object: org.apache.spark.examples.SparkPi)
* ```master```: master url for the cluster (local, local[2], local[*] , spark://23.195.26.187:7077, mesos://$IP:$PORT, yarn, k8s://HOST:PORT). <a href="https://spark.apache.org/docs/latest/submitting-applications.html"> More info </a>
* ```deploy-mode```: cluster (deploy driver on the worker nodes), client (default, deploy driver locally as an external client).
* ```conf```: key-value pairs.
* ```application-jar```: path to the jar with the dependencies (it can be  hdfs://, or file://, but in the second case, the file must be visible for all workers).
* ```application-arguments```: arguments that the main class is expecting to have.

# Spark API's (RDDs, DataFrames, Datasets, SQL Tables)<a name="apis"></a>
Spark API's are composed of unstructure APIs (RDDs), and structure APIs (DF, DS).

# Dataframes
rows with schema. Data is located in multiple machines.

# Partitions
Spark breaks data into chunks called partitions. A partition if a collection of rows store in machines. 
One partition means no parallel computing
Multiple partitions and one computer, means no parallelism.

# Transformations
<b>NARROW: </b> 1 to 1 (map, filter, union, sample, read). With this ops, spark performs an operation called ```pipelining```, which performs ops in mem. This transformations are simple, they dont need lot of intelligence or context<br>
<b>WIDE: </b> many to one, many to many, not 1 to 1. Involves shuffle operations (all aggregations, join, repartition, coalesce, sort). When perform a shuffle, spark writes data to disk. This transformartion need context, intelligence, look at the data first and then make the operation.<br>
Tranformations are lazy, they are not applied until an action is executed.
Lazy evaluation leads to make an execution plan, which will be optimized by spark. Transformations just help to build the logical plan.

# Actions
Action trigguer the computation of the logical plan. There are 3 kids:
* Actions to view data in console
* Actions to collect data into native objects
* Actions to write data

Examples: write, count (without aggregation), show, collect, countByValue, reduce, fold, aggregate, foreach.

# Execution Plan
The top or the first line of the execution plan, correspond to the end result, and the bottom, to the first operations applied. 

# Shufle
By default is 200. in order to change this value
```scala
spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2) //  Array([United States,Singapore,1], [Moldova,United States,1])
```
# DataFrames and SQL
You can express your business logic in SQL or DataFrames. You can register any dataframe as table or view, and query using pure SQL. 
There is no performance difference in using SQL/DF, they both compile to the same logical plan.
```scala
flightData2015.createOrReplaceTempView("flight_data_2015")

val dataFrameWay = flightData2015
.groupBy('DEST_COUNTRY_NAME)
.count()
```

Always think in terms of <b>DAG transformations</b>. 

# Datasets
Datasets are type-safe versions. Type-safe because you assign Java/Scala class to the records within the DataFrame, and you can manipullate them as collection of typed-objects. You always need a dataframe first, in order to convert it as Dataset. 
```scala
case class Flight(
    DEST_COUNTRY_NAME: String,
    ORIGIN_COUNTRY_NAME: String,
    count: BigInt)
val flights = flightsDF.as[Flight]
flights
.take(5)
.filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
.map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))
```

