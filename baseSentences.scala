/*
Create DataFrame from RDD with a schema
*/

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


/*
Create DataFrame with Columns with infered schema
*/

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
 
 
 /*
 Declare an spark object for execution
 */
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
