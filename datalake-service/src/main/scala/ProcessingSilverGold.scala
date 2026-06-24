import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcessingSilverGold {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ProcessingSilverGold")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val silverDF = spark.read
      .format("avro")
      .load("../data/silver/")
    val bpmPerHour = silverDF
      .withColumn("hour", from_unixtime(col("timestamp"), "yyyy-MM-dd HH"))
      .groupBy("device_id", "hour")
      .agg(
        avg("bpm").alias("avg_bpm"),
        max("bpm").alias("max_bpm"),
        min("bpm").alias("min_bpm")
      )
    val stepsPerDay = silverDF
      .withColumn("day", from_unixtime(col("timestamp"), "yyyy-MM-dd"))
      .groupBy("device_id", "day")
      .agg(
        sum("steps").alias("total_steps")
      )
    val goldDF = bpmPerHour
      .withColumn("day", col("hour").substr(0, 10))
      .join(stepsPerDay, Seq("device_id", "day"), "left")
      .drop("day")
    goldDF.write
      .format("parquet")
      .mode("overwrite")
      .save("../data/gold/")

    println(s"Silver -> Gold : ${goldDF.count()} lignes écrites")

    spark.stop()
  }
}
