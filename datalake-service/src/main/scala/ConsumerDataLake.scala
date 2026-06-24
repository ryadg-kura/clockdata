import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ConsumerDataLake {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ConsumerDataLake")
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

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "clockdata")
      .option("startingOffsets", "earliest")
      .load()

    import spark.implicits._
    val messages = kafkaStream
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema).as("data"))
      .select("data.*")

    val query = messages.writeStream
      .format("json")
      .option("path", "../data/bronze/")
      .option("checkpointLocation", "../data/bronze/checkpoint/")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}