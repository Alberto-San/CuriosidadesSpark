# Table of contents
1. [Basics] (#basics)
2. [What is a STAGE?](#stage)
3. [What is a TASK?] (#task)
4. [What is a JOB?] (#job)
5. [Example] (#example)
6. [Transformations] (#transformations)
7. [Actions] (#actions)
8. [Driver] (#driver)
9. [Spark Architecture] (#architecture)
10. [Spark Optimizations] (#optimizations)

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

  
