import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

final case class SilverRecord(
  device_id: String,
  timestamp: Long,
  bpm: Int,
  steps: Int,
  lat: Double,
  lng: Double
)

final case class Answer(question: String, answer: String)

final case class PeriodCount(period: String, alerts: Long)
final case class DeviceCount(device_id: String, alerts: Long)
final case class HourAvg(hour_of_day: Int, avg_bpm: Double)
final case class DeviceSteps(device_id: String, total_steps: Long)

object Analysis {

  private val CriticalBpm = 40

  /** Q1 : y a-t-il plus d'alertes critiques en semaine ou le week-end ? */
  def weekdayVsWeekendAlerts(data: Dataset[SilverRecord])(implicit spark: SparkSession): Answer = {
    import spark.implicits._

    val counts = data
      .filter($"bpm" < CriticalBpm)
      .withColumn("day_name", date_format(from_unixtime($"timestamp"), "E"))
      .withColumn("period", when($"day_name".isin("Sat", "Sun"), "week-end").otherwise("semaine"))
      .groupBy($"period")
      .count()
      .withColumnRenamed("count", "alerts")
      .as[PeriodCount]
      .collect()
      .toList

    val weekday = counts.find(_.period == "semaine").map(_.alerts).getOrElse(0L)
    val weekend = counts.find(_.period == "week-end").map(_.alerts).getOrElse(0L)

    val verdict =
      if (weekday > weekend) s"davantage en semaine ($weekday contre $weekend le week-end)"
      else if (weekend > weekday) s"davantage le week-end ($weekend contre $weekday en semaine)"
      else s"autant en semaine qu'en week-end ($weekday chacun)"

    Answer(
      "Y a-t-il plus d'alertes critiques (BPM < 40) en semaine ou le week-end ?",
      s"Il y a $verdict."
    )
  }

  /** Q2 : quel appareil déclenche le plus d'alertes critiques ? */
  def deviceWithMostAlerts(data: Dataset[SilverRecord])(implicit spark: SparkSession): Answer = {
    import spark.implicits._

    val ranking = data
      .filter($"bpm" < CriticalBpm)
      .groupBy($"device_id")
      .count()
      .withColumnRenamed("count", "alerts")
      .orderBy(desc("alerts"))
      .as[DeviceCount]
      .collect()
      .toList

    val text = ranking.headOption
      .map(top => s"${top.device_id} avec ${top.alerts} alerte(s)")
      .getOrElse("aucun (pas d'alerte critique enregistrée)")

    Answer("Quel appareil déclenche le plus d'alertes critiques (BPM < 40) ?", s"C'est $text.")
  }

  /** Q3 : à quelle heure de la journée le BPM moyen est-il le plus élevé ? */
  def peakActivityHour(data: Dataset[SilverRecord])(implicit spark: SparkSession): Answer = {
    import spark.implicits._

    val ranking = data
      .withColumn("hour_of_day", hour(from_unixtime($"timestamp")))
      .groupBy($"hour_of_day")
      .agg(avg($"bpm").alias("avg_bpm"))
      .orderBy(desc("avg_bpm"))
      .as[HourAvg]
      .collect()
      .toList

    val text = ranking.headOption
      .map(top => f"${top.hour_of_day}%02dh (BPM moyen : ${top.avg_bpm}%.1f)")
      .getOrElse("indéterminée (pas de données)")

    Answer("À quelle heure de la journée le BPM moyen est-il le plus élevé ?", s"C'est à $text.")
  }

  /** Q4 : quel appareil (porteur) a marché le plus de pas ? */
  def mostActiveDevice(data: Dataset[SilverRecord])(implicit spark: SparkSession): Answer = {
    import spark.implicits._

    val ranking = data
      .groupBy($"device_id")
      .agg(max($"steps").alias("total_steps"))
      .orderBy(desc("total_steps"))
      .as[DeviceSteps]
      .collect()
      .toList

    val text = ranking.headOption
      .map(top => s"${top.device_id} avec ${top.total_steps} pas")
      .getOrElse("indéterminé (pas de données)")

    Answer("Quel appareil (porteur) a marché le plus de pas ?", s"C'est $text.")
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Analysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    val silver: Dataset[SilverRecord] = spark.read
      .format("avro")
      .load("../data/silver/")
      .as[SilverRecord]

    val answers = List(
      weekdayVsWeekendAlerts(silver),
      deviceWithMostAlerts(silver),
      peakActivityHour(silver),
      mostActiveDevice(silver)
    )

    println("\n===== ClockData — Analyse (4 questions) =====\n")
    answers.zipWithIndex.foreach { case (a, i) =>
      println(s"${i + 1}. ${a.question}")
      println(s"   -> ${a.answer}\n")
    }

    answers.toDF().write.mode("overwrite").json("../data/gold/analysis/")
    println("Réponses sauvegardées dans ../data/gold/analysis/")

    spark.stop()
  }
}
