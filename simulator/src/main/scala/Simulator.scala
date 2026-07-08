import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

/** État immuable d'une montre simulée. */
final case class Watch(deviceId: String, bpm: Int, steps: Int, lat: Double, lng: Double)

object Simulator {

  val BaseLat: Double = 48.7
  val BaseLng: Double = 2.4
  val Topic: String = "clockdata"

  def clamp(value: Int, low: Int, high: Int): Int =
    math.max(low, math.min(high, value))

  def round4(value: Double): BigDecimal =
    BigDecimal(value).setScale(4, RoundingMode.HALF_UP)

  /** Crée une montre avec un état initial plausible. */
  def initWatch(id: String, rnd: Random): Watch =
    Watch(
      deviceId = id,
      bpm = 60 + rnd.nextInt(31),
      steps = rnd.nextInt(5000),
      lat = BaseLat + (rnd.nextDouble() - 0.5) * 0.1,
      lng = BaseLng + (rnd.nextDouble() - 0.5) * 0.1
    )

  /**
   * Fait évoluer une montre d'un "cran".
   * Avec une probabilité "anomalyRate", génère un BPM critique (< 40)
   * pour rendre testable le composant d'alerte en aval.
   */
  def tick(w: Watch, anomalyRate: Double, rnd: Random): Watch = {
    val newBpm =
      if (rnd.nextDouble() < anomalyRate) 25 + rnd.nextInt(15) // 25..39, critique
      else clamp(w.bpm + rnd.nextInt(9) - 4, 50, 180)
    w.copy(
      bpm = newBpm,
      steps = w.steps + rnd.nextInt(26),
      lat = w.lat + (rnd.nextDouble() - 0.5) * 0.001,
      lng = w.lng + (rnd.nextDouble() - 0.5) * 0.001
    )
  }

  /** Sérialise au format JSON attendu par les consumers et le data lake. */
  def toJson(w: Watch, ts: Long): String =
    s"""{"device_id":"${w.deviceId}","timestamp":$ts,"bpm":${w.bpm},""" +
      s""""steps":${w.steps},"lat":${round4(w.lat)},"lng":${round4(w.lng)}}"""

  def main(args: Array[String]): Unit = {
    val bootstrap   = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
    val devices     = sys.env.getOrElse("SIM_DEVICES", "3").toIntOption.getOrElse(3)
    val intervalMs  = sys.env.getOrElse("SIM_INTERVAL_MS", "2000").toLongOption.getOrElse(2000L)
    val anomalyRate = sys.env.getOrElse("SIM_ANOMALY_RATE", "0.02").toDoubleOption.getOrElse(0.02)

    val rnd = new Random()

    // Configuration du producteur sous forme de Map immuable (convertie en Java
    // pour le client Kafka), plutôt qu'un java.util.Properties mutable.
    val config = Map[String, AnyRef](
      "bootstrap.servers" -> bootstrap,
      "key.serializer"    -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer"  -> "org.apache.kafka.common.serialization.StringSerializer",
      "acks"              -> "all"
    )

    val producer = new KafkaProducer[String, String](config.asJava)

    val initial: List[Watch] =
      (1 to devices).toList.map(i => initWatch(f"device_$i%03d", rnd))

    println(s"[simulator] Envoi vers '$Topic' ($devices montre(s), toutes les ${intervalMs}ms).")

    @tailrec
    def loop(watches: List[Watch]): Unit = {
      val now  = System.currentTimeMillis() / 1000
      val next = watches.map(w => tick(w, anomalyRate, rnd))
      next.foreach { w =>
        val json = toJson(w, now)
        producer.send(new ProducerRecord[String, String](Topic, w.deviceId, json))
        println(json)
      }
      producer.flush()
      Thread.sleep(intervalMs)
      loop(next)
    }

    loop(initial)
  }
}
