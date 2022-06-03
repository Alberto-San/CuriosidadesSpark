/******************************************************************************************************************
Create DataFrame from RDD with a schema
******************************************************************************************************************/

val someData = Seq(
  Row(8, "bat"),
  Row(64, "mouse"),
  Row(-27, "horse")
)

val someSchema = List(
  StructField("number", IntegerType, true),
  StructField("word", StringType, true)
)

val someDF = spark.createDataFrame(
  spark.sparkContext.parallelize(someData),
  StructType(someSchema)
)


/*******************************************************************************************************************
Create DataFrame with Columns with infered schema
*******************************************************************************************************************/

val cars = Seq(
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    ("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15,8,429,198,4341,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14,8,454,220,4354,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14,8,440,215,4312,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14,8,455,225,4425,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15,8,390,190,3850,8.5,"1970-01-01","USA")
  )
  
 import spark.implicits._
 val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")
 
 
 /*******************************************************************************************************************
 Declare an spark object for execution
 *******************************************************************************************************************/
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
object DataFramesBasics extends App {
   val spark = SparkSession.builder()
      .appName("DataFrames Basics")
      .config("spark.master", "local")
      .getOrCreate()
  //Spark Code ....
 }


/*
Read from a postgress db:
1. Import the library from sbt or maven
....
libraryDependencies ++= Seq(
  ...
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)
*/

// Reading from a remote DB
val driver = "org.postgresql.Driver"
val url = "jdbc:postgresql://localhost:5432/rtjvm"
val user = "docker"
val password = "docker"

val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver) // 
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

//Reading dB executing query
val query = "(SELECT * FROM public.employees) tmp"
val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver) // 
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

/*
Read dates with custom format type
*/

spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")


/*******************************************************************************************************************
Columns and expressions
*******************************************************************************************************************/

// various select methods
import spark.implicits._
carsDF.select(
  carsDF.col("Name"),
  col("Acceleration"),
  column("Weight_in_lbs"),
  'Year, // Scala Symbol, auto-converted to column
  $"Horsepower", // fancier interpolated string, returns a Column object
  expr("Origin") // EXPRESSION
)

val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")
val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema
val moviesProfitDF2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )
val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
carsWithColumnRenamed.drop("Cylinders", "Displacement")



/*******************************************************************************************************************
Aggregations
*******************************************************************************************************************/

// counting
val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
moviesDF.selectExpr("count(Major_Genre)")

// counting all
moviesDF.select(count("*")) // count all the rows, and will INCLUDE nulls

// counting distinct
moviesDF.select(countDistinct(col("Major_Genre"))).show()

// approximate count
moviesDF.select(approx_count_distinct(col("Major_Genre")))

val countByGenreDF = moviesDF
  .groupBy(col("Major_Genre")) // includes null
  .count()  // select count(*) from moviesDF group by Major_Genre

val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)

/*******************************************************************************************************************
join types
*******************************************************************************************************************/
val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
guitaristsDF.join(bandsDF, joinCondition, "left_outer") // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
guitaristsDF.join(bandsDF, joinCondition, "right_outer") // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
guitaristsDF.join(bandsDF, joinCondition, "outer") // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
guitaristsDF.join(bandsDF, joinCondition, "left_semi") // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
guitaristsDF.join(bandsDF, joinCondition, "left_anti") // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId")) //default inner
guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")) // using complex types


/*******************************************************************************************************************
Common Types: Boilerplate
*******************************************************************************************************************/
// correlation = number between -1 and 1
println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an ACTION */)
carsDF.select(initcap(col("Name"))) // capitalization: initcap, lower, upper
carsDF.select("*").where(col("Name").contains("volkswagen")) // contains
//The fold method takes two sets of arguments. One contains a start value and the other a combining function.
val list = List(1, 2, 3, 4, 5)
val sum = list.fold(0)((x, y) => x + y)
assert(sum == 15)
def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")
val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name)) //List(contains(Name, volkswagen), contains(Name, mercedes-benz), contains(Name, ford))
val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter) //(((false OR contains(Name, volkswagen)) OR contains(Name, mercedes-benz)) OR contains(Name, ford))

/*******************************************************************************************************************
Complex data types: pyspark
*******************************************************************************************************************/
val moviesWithReleaseDates = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) // conversion
moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub

// Structures
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]

structureSchema = StructType([
        StructField("name", StructType([
             StructField("firstname", StringType(), True),
             StructField("middlename", StringType(), True),
             StructField("lastname", StringType(), True)
             ])),
         StructField("id", StringType(), True),
         StructField("gender", StringType(), True),
         StructField("salary", IntegerType(), True)
         ])
df_struct = spark.createDataFrame(data=structureData,schema=structureSchema)
df_struct.show(truncate=False)
/*
root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)

+--------------------+-----+------+------+
|name                |id   |gender|salary|
+--------------------+-----+------+------+
|{James, , Smith}    |36636|M     |3100  |
|{Michael, Rose, }   |40288|M     |4300  |
|{Robert, , Williams}|42114|M     |1400  |
|{Maria, Anne, Jones}|39192|F     |5500  |
|{Jen, Mary, Brown}  |     |F     |-1    |
+--------------------+-----+------+------+
*/

df_struct.select("name.*", "id","gender","salary").printSchema()
df_struct_flatten=df_struct.select("name.*", "id","gender","salary")

df_struct_flatten.show(truncate=False)

/*
root
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)

+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|id   |gender|salary|
+---------+----------+--------+-----+------+------+
|James    |          |Smith   |36636|M     |3100  |
|Michael  |Rose      |        |40288|M     |4300  |
|Robert   |          |Williams|42114|M     |1400  |
|Maria    |Anne      |Jones   |39192|F     |5500  |
|Jen      |Mary      |Brown   |     |F     |-1    |
+---------+----------+--------+-----+------+------+
*/

// Arrays

df_array = spark.createDataFrame([ \
    Row(arrayA=[1,2,3,4,5],fieldB="Rory"),Row(arrayA=[888,777,555,999,666],fieldB="Arin")])
/*
df_array >>
+--------------------+------+
|              arrayA|fieldB|
+--------------------+------+
|     [1, 2, 3, 4, 5]|  Rory|
|[888, 777, 555, 9...|  Arin|
+--------------------+------+
*/
df_finding_in_array = df_array.filter(array_contains(df_array.arrayA,3))
df_finding_in_array.show()
/*
+---------------+------+
|         arrayA|fieldB|
+---------------+------+
|[1, 2, 3, 4, 5]|  Rory|
+---------------+------+
*/

df_array.select( \
        col("arrayA").getItem(0).alias("element0"), \
        col("arrayA")[4].alias("element5"), \
        col("fieldB")) \
    .show()
/*
+--------+--------+------+
|element0|element5|fieldB|
+--------+--------+------+
|       1|       5|  Rory|
|     888|     666|  Arin|
+--------+--------+------+
*/

df_nested_arraytype = spark.createDataFrame([
    Row(
        arrayA=[
            Row(childStructB=Row(field1=1, field2="foo")),
            Row(childStructB=Row(field1=2, field2="bar"))
        ]
    )])
df_nested_arraytype.printSchema()

df_nested_arraytype.show(1, False)

/*
root
 |-- arrayA: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- childStructB: struct (nullable = true)
 |    |    |    |-- field1: long (nullable = true)
 |    |    |    |-- field2: string (nullable = true)

+------------------------+
|arrayA                  |
+------------------------+
|[{{1, foo}}, {{2, bar}}]|
+------------------------+
*/

/*

'''Printing arrayA, field1 and field2 using dot '''
df_child = df_nested_arraytype.select(
        "arrayA.childStructB.field1",
        "arrayA.childStructB.field2")

df_child.printSchema()

df_child.show()
/*
root
 |-- field1: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- field2: array (nullable = true)
 |    |-- element: string (containsNull = true)

+------+----------+
|field1|    field2|
+------+----------+
|[1, 2]|[foo, bar]|
+------+----------+
*/
'''Nested structype within nested arraytype'''
df_nested_B = spark.createDataFrame([
    Row(
        arrayA=[[
            Row(childStructB=Row(field1=1, field2="foo")),
            Row(childStructB=Row(field1=2, field2="bar"))
        ]]
    )])
df_nested_B.printSchema()
df_nested_B.show(1, False)
/*
root
 |-- arrayA: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- childStructB: struct (nullable = true)
 |    |    |    |    |-- field1: long (nullable = true)
 |    |    |    |    |-- field2: string (nullable = true)

+--------------------------+
|arrayA                    |
+--------------------------+
|[[{{1, foo}}, {{2, bar}}]]|
+--------------------------+
*/

// Working with structs and arrays
arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)

/*
root
 |-- name: string (nullable = true)
 |-- subjects: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: string (containsNull = true)

+-------+-----------------------------------+
|name   |subjects                           |
+-------+-----------------------------------+
|James  |[[Java, Scala, C++], [Spark, Java]]|
|Michael|[[Spark, Java, C++], [Spark, Java]]|
|Robert |[[CSharp, VB], [Spark, Python]]    |
+-------+-----------------------------------+
*/
// Explode
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import flatten
from pyspark.sql.functions import arrays_zip, col
from functools import reduce
from pyspark.sql import DataFrame
df_explode=df.select(df.name,explode(df.subjects).alias("Exploded_Subjects"))
df_explode.show(truncate=False)
/*
root
 |-- name: string (nullable = true)
 |-- Exploded_Subjects: array (nullable = true)
 |    |-- element: string (containsNull = true)

+-------+------------------+
|name   |Exploded_Subjects |
+-------+------------------+
|James  |[Java, Scala, C++]|
|James  |[Spark, Java]     |
|Michael|[Spark, Java, C++]|
|Michael|[Spark, Java]     |
|Robert |[CSharp, VB]      |
|Robert |[Spark, Python]   |
+-------+------------------+
*/

//Flatten
df_flatten=df.select(df.name,flatten(df.subjects).alias("Flattened_Subjects"))
df_flatten.printSchema()
df_flatten.show(truncate=False)

/*
root
 |-- name: string (nullable = true)
 |-- Flattened_Subjects: array (nullable = true)
 |    |-- element: string (containsNull = true)

+-------+-------------------------------+
|name   |Flattened_Subjects             |
+-------+-------------------------------+
|James  |[Java, Scala, C++, Spark, Java]|
|Michael|[Spark, Java, C++, Spark, Java]|
|Robert |[CSharp, VB, Spark, Python]    |
+-------+-------------------------------+
*/

//zipp Array and explode
df_flatten_zip=df_flatten \
               .withColumn("zippedArray", arrays_zip("Flattened_Subjects"))  \
               .withColumn("explodeZipped", explode("zippedArray")) \
               .withColumn("data", col("explodeZipped.Flattened_Subjects"))

df_flatten_zip.printSchema()
df_flatten_zip.show(truncate=False)
/*
+-------+-------------------------------+-----------------------------------------+-------------+------+
|name   |Flattened_Subjects             |zippedArray                              |explodeZipped|data  |
+-------+-------------------------------+-----------------------------------------+-------------+------+
|James  |[Java, Scala, C++, Spark, Java]|[{Java}, {Scala}, {C++}, {Spark}, {Java}]|{Java}       |Java  |
|James  |[Java, Scala, C++, Spark, Java]|[{Java}, {Scala}, {C++}, {Spark}, {Java}]|{Scala}      |Scala |
|James  |[Java, Scala, C++, Spark, Java]|[{Java}, {Scala}, {C++}, {Spark}, {Java}]|{C++}        |C++   |
|James  |[Java, Scala, C++, Spark, Java]|[{Java}, {Scala}, {C++}, {Spark}, {Java}]|{Spark}      |Spark |
|James  |[Java, Scala, C++, Spark, Java]|[{Java}, {Scala}, {C++}, {Spark}, {Java}]|{Java}       |Java  |
|Michael|[Spark, Java, C++, Spark, Java]|[{Spark}, {Java}, {C++}, {Spark}, {Java}]|{Spark}      |Spark |
|Michael|[Spark, Java, C++, Spark, Java]|[{Spark}, {Java}, {C++}, {Spark}, {Java}]|{Java}       |Java  |
|Michael|[Spark, Java, C++, Spark, Java]|[{Spark}, {Java}, {C++}, {Spark}, {Java}]|{C++}        |C++   |
|Michael|[Spark, Java, C++, Spark, Java]|[{Spark}, {Java}, {C++}, {Spark}, {Java}]|{Spark}      |Spark |
|Michael|[Spark, Java, C++, Spark, Java]|[{Spark}, {Java}, {C++}, {Spark}, {Java}]|{Java}       |Java  |
|Robert |[CSharp, VB, Spark, Python]    |[{CSharp}, {VB}, {Spark}, {Python}]      |{CSharp}     |CSharp|
|Robert |[CSharp, VB, Spark, Python]    |[{CSharp}, {VB}, {Spark}, {Python}]      |{VB}         |VB    |
|Robert |[CSharp, VB, Spark, Python]    |[{CSharp}, {VB}, {Spark}, {Python}]      |{Spark}      |Spark |
|Robert |[CSharp, VB, Spark, Python]    |[{CSharp}, {VB}, {Spark}, {Python}]      |{Python}     |Python|
+-------+-------------------------------+-----------------------------------------+-------------+------+
*/


//Maps
data = [({'a': 1, 'b': 2},), ({'c':3},), ({'a': 4, 'c': 5},)]
df_map = spark.createDataFrame(data, ["column"])
df_map.printSchema()
df_map.show()
/*
root
 |-- column: map (nullable = true)
 |    |-- key: string
 |    |-- value: long (valueContainsNull = true)

+----------------+
|          column|
+----------------+
|{a -> 1, b -> 2}|
|        {c -> 3}|
|{a -> 4, c -> 5}|
+----------------+
*/

from pyspark.sql.session import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as f
''' Flattening map'''
df_new = df_map.select(
    f.struct(*[f.col("column").getItem(c).alias(c) for c in ["a", "b", "c"]]).alias("a")
)
df_new.show()
/*
root
 |-- a: struct (nullable = false)
 |    |-- a: long (nullable = true)
 |    |-- b: long (nullable = true)
 |    |-- c: long (nullable = true)

+---------------+
|              a|
+---------------+
|   {1, 2, null}|
|{null, null, 3}|
|   {4, null, 5}|
+---------------+
*/

'''Flattening maptype to rows'''
df = df_map.withColumn("id", f.monotonically_increasing_id())\
    .select("id", f.explode("column"))\
    .groupby("id")\
    .pivot("key")\
    .agg(f.first("value"))\
    .drop("id")\

df.printSchema()
df.show()
/*
root
 |-- a: long (nullable = true)
 |-- b: long (nullable = true)
 |-- c: long (nullable = true)

+----+----+----+
|   a|   b|   c|
+----+----+----+
|   4|null|   5|
|null|null|   3|
|   1|   2|null|
+----+----+----+
*/
*/

// 1 - with col operators
moviesDF
  .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
  .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

// 2 - with expression strings
moviesDF
  .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
  .selectExpr("Title", "Profit.US_Gross")

val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // ARRAY of strings
moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"), // indexing
    size(col("Title_Words")), // array size
    array_contains(col("Title_Words"), "Love") // look for value in array
  )


/*******************************************************************************************************************
Datasets
DataFrame actually enjoys better performance than Dataset. The reason for this is that Spark can understand the internals of the built-in functions associated with DataFrame and this enables the Catalyst optimization (rearrange and change the execution tree) as well as performing wholestage codegen to avoid a lot of the virtualization.
Another advantage of Dataframe is that it's schema is set at run time rather than at compile time. This means that if you read for example from a parquet file, the schema would be set by the content of the file. This enables to handle dynamic cases (e.g. to perform ETL)
https://stackoverflow.com/questions/54019955/why-dataframe-still-there-in-spark-2-2-also-even-dataset-gives-more-performance
Datasets is an intermedia between RDDs and DataFrames, compiles everything in terms of logical JVM objects.
When it comes to serializing data, the Dataset API has the concept of encoders which translate between JVM representations (objects) and Spark’s internal binary format. 
https://stackoverflow.com/questions/31508083/difference-between-dataframe-dataset-and-rdd-in-spark
*******************************************************************************************************************/

def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")
val carsDF = readDF("cars.json")
/*
root
 |-- Acceleration: double (nullable = true)
 |-- Cylinders: long (nullable = true)
 |-- Displacement: double (nullable = true)
 |-- Horsepower: long (nullable = true)
 |-- Miles_per_Gallon: double (nullable = true)
 |-- Name: string (nullable = true)
 |-- Origin: string (nullable = true)
 |-- Weight_in_lbs: long (nullable = true)
 |-- Year: string (nullable = true)
+------------+---------+------------+----------+----------------+-------------------------+------+-------------+----------+
|Acceleration|Cylinders|Displacement|Horsepower|Miles_per_Gallon|Name                     |Origin|Weight_in_lbs|Year      |
+------------+---------+------------+----------+----------------+-------------------------+------+-------------+----------+
|12.0        |8        |307.0       |130       |18.0            |chevrolet chevelle malibu|USA   |3504         |1970-01-01|
|11.5        |8        |350.0       |165       |15.0            |buick skylark 320        |USA   |3693         |1970-01-01|
+------------+---------+------------+----------+----------------+-------------------------+------+-------------+----------+
*/

//making a DataSet
case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )
import spark.implicits._
val carsDS = carsDF.as[Car]
carsDS.show(false)
carsDS.printSchema()
/*
root
 |-- Acceleration: double (nullable = true)
 |-- Cylinders: long (nullable = true)
 |-- Displacement: double (nullable = true)
 |-- Horsepower: long (nullable = true)
 |-- Miles_per_Gallon: double (nullable = true)
 |-- Name: string (nullable = true)
 |-- Origin: string (nullable = true)
 |-- Weight_in_lbs: long (nullable = true)
 |-- Year: string (nullable = true)
+------------+---------+------------+----------+----------------+-------------------------+------+-------------+----------+
|Acceleration|Cylinders|Displacement|Horsepower|Miles_per_Gallon|Name                     |Origin|Weight_in_lbs|Year      |
+------------+---------+------------+----------+----------------+-------------------------+------+-------------+----------+
|12.0        |8        |307.0       |130       |18.0            |chevrolet chevelle malibu|USA   |3504         |1970-01-01|
|11.5        |8        |350.0       |165       |15.0            |buick skylark 320        |USA   |3693         |1970-01-01|
+------------+---------+------------+----------+----------------+-------------------------+------+-------------+----------+
*/
//With Datasets you could use RDD functions, like comprehensions
// map, flatMap, fold, reduce, for comprehensions ...
val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
carNamesDS.show(false)
carNamesDS.printSchema()
/*
+-------------------------+
|value                    |
+-------------------------+
|CHEVROLET CHEVELLE MALIBU|
|BUICK SKYLARK 320        |
+-------------------------+
only showing top 2 rows

root
 |-- value: string (nullable = true)
*/

/**
* Exercises
*
* 1. Count how many cars we have: val carsCount = carsDS.count
* 2. Count how many POWERFUL cars we have (HP > 140): arsDS.filter(_.Horsepower.getOrElse(0L) > 140).count
* 3. Average HP for the entire dataset: 
    3.1 carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount
    3.2 carsDS.select(avg(col("Horsepower")))
*/
case class Guitar(id: Long, make: String, model: String, guitarType: String)
case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
case class Band(id: Long, name: String, hometown: String, year: Long)
val guitarsDS = readDF("guitars.json").as[Guitar]
val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
val bandsDS = readDF("bands.json").as[Band]
val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()
carsGroupedByOrigin.select(col("Name").as[String]).collect()

