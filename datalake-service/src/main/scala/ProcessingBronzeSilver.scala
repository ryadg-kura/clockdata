import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProcessingBronzeSilver {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ProcessingBronzeSilver")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schema = StructType(Seq(
      StructField("device_id", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("bpm", IntegerType, nullable = false),
      StructField("steps", IntegerType, nullable = false),
      StructField("lat", DoubleType, nullable = false),
      StructField("lng", DoubleType, nullable = false)
    ))
    val bronzeDF = spark.read
      .schema(schema)
      .json("../data/bronze/")
    val silverDF = bronzeDF
      .filter(col("device_id").isNotNull)
      .filter(col("timestamp").isNotNull)
      .filter(col("bpm").isNotNull && col("bpm") > 0 && col("bpm") < 300)
      .filter(col("steps").isNotNull && col("steps") >= 0)
      .filter(col("lat").isNotNull && col("lng").isNotNull)
    silverDF.write
      .format("avro")
      .mode("overwrite")
      .save("../data/silver/")

    println(s"Bronze -> Silver : ${silverDF.count()} lignes écrites")

    spark.stop()
  }
}
