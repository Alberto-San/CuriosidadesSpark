case class Guitar(
                   configurationId: String,
                   make: String,
                   model: String,
                   soundScore: Double 
                 )

case class GuitarSale(
                       registration: String,
                       make: String,
                       model: String,
                       soundScore: Double, 
                       salePrice: Double
                     )

val random = new Random()
def randomSoundQuality(): Double = {
    s"4.${random.nextInt(9)}".toDouble}
def randomGuitarRegistration(): String = {
    randomString(8)}
def randomGuitarModelType(): String = {
    s"${randomString(4)}-${randomString(4)}"}
def randomGuitarPrice(): Int = {
    500 + random.nextInt(1500)}
def randomGuitarSale(uniformDist: Boolean = false): GuitarSale = {
    val makeModel = randomGuitarModel(uniformDist)
    GuitarSale(randomGuitarRegistration(), makeModel._1, makeModel._2, randomSoundQuality(), randomGuitarPrice())
  }
def randomGuitar(uniformDist: Boolean = false): Guitar = {
    val makeModel = randomGuitarModel(uniformDist)
    Guitar(randomGuitarModelType(), makeModel._1, makeModel._2, randomSoundQuality())
  }
def randomGuitarModel(uniform: Boolean = false): (String, String) = {
    val makeModelIndex = if (!uniform && random.nextBoolean()) 0 else random.nextInt(guitarModelSet.size)
    guitarModelSet(makeModelIndex)
  }
def randomDouble(limit: Double): Double = {
    assert(limit >= 0)
    random.nextDouble() * limit
 }
def randomLong(limit: Long = Long.MaxValue): Long = {
    assert(limit >= 0)
    Math.abs(random.nextLong()) % limit
 }
def randomInt(limit: Int = Int.MaxValue): Int = {
    assert(limit >= 0)
    random.nextInt(limit)
 }
def randomIntBetween(low: Int, high: Int) = {
    assert(low <= high)
    random.nextInt(high - low) + low
 }
def randomString(n: Int) = {
    random.alphanumeric.take(n).mkString("")
 }
def pickFrom[T](seq: Seq[T]): T = {
    assert(seq.nonEmpty)
    seq(randomInt(seq.length))
 }

val guitarModelSet: Seq[(String, String)] = 
    Seq(
        ("Taylor", "914"),
        ("Martin", "D-18"),
        ("Takamine", "P7D"),
        ("Gibson", "L-00"),
        ("Tanglewood", "TW45"),
        ("Fender", "CD-280S"),
        ("Yamaha", "LJ16BC")
    )
val guitars: Dataset[Guitar] = Seq.fill(40000)(randomGuitar).toDS
val guitarSales: Dataset[GuitarSale] = Seq.fill(20000)(randomGuitarSale).toDS 
def naiveSolution(): BigInt = {
    /*
    A Guitar is similar to a GuitarSale if
    - same make and model
    - abs(guitar.soundScore - guitarSale.soundScore) <= 0.1
    compute avg(sale prices of ALL GuitarSales
    */
    val joined = guitars.join(guitarSales, Seq("make", "model"))
      .where(abs(guitarSales("soundScore") - guitars("soundScore")) <= 0.1)
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))

    joined.explain()
    joined.count()
  }

/*
The join task will take the most time due to one of the task takes long time (struggler task). The reason of that if because that task contains a disproportion amount of data. 
Spark allocate data with the same (make, model) executor, this task has a disproportional combination of the same make and model (data is generated using randomGuitar). which uses randomGuitarModel which hash 50% prob that returns 0. Our data is not uniformed distributed in terms of make and model. This is called data skew, you can see data skew as one task in a stage that is far away from the median. A general way for solved for any kind of data skew is doing the following 
*/

def noSkewSolution() = {
    // salting interval 0-99
    /*
    Salting adds noise to the data in order to become it uniformed distributed.
    */
    val explodedGuitars = guitars.withColumn("salt", explode(lit((0 to 99).toArray))) // multiplying the guitars DS x100
    val saltedGuitarSales = guitarSales.withColumn("salt", monotonically_increasing_id() % 100) // mod by 100.

    val nonSkewedJoin = explodedGuitars.join(saltedGuitarSales, Seq("make", "model", "salt")) // the combination of columns will more more uniformely distributed. 
      .where(abs(saltedGuitarSales("soundScore") - explodedGuitars("soundScore")) <= 0.1)
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))

    nonSkewedJoin.explain()
    nonSkewedJoin.count()
  }

/*
Fixing data skews is not solvable adding more resources. 
this problem can be solved using salt. 
The larger the salt interval, the less skewed taks. 
The larger the salt interval, the larger the shuffle size.
Also works for aggregations. 
*/


