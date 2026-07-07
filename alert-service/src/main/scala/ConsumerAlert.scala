import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Duration
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object ConsumerAlert {

  val InputTopic: String = "clockdata"
  val OutputTopic: String = "alerts"
  val CriticalBpmThreshold: Int = 40

  /** Retourne true si le BPM est critique (inférieur au seuil). */
  def shouldAlert(bpm: Int): Boolean = bpm < CriticalBpmThreshold

  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")

    val consumerConfig = Map[String, AnyRef](
      "bootstrap.servers" -> bootstrap,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "alert-detector",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )

    val producerConfig = Map[String, AnyRef](
      "bootstrap.servers" -> bootstrap,
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )

    val consumer = new KafkaConsumer[String, String](consumerConfig.asJava)
    val producer = new KafkaProducer[String, String](producerConfig.asJava)
    consumer.subscribe(java.util.List.of(InputTopic))

    println(s"[ConsumerAlert] En écoute sur le topic '$InputTopic'...")

    @tailrec
    def loop(): Unit = {
      val records = consumer.poll(Duration.ofMillis(1000))
      records.asScala.foreach { record =>
        val json = record.value()
        Reading.parse(json) match {
          case Some(reading) =>
            if (shouldAlert(reading.bpm)) {
              println(s"[ALERTE] BPM critique : ${reading.bpm} — device ${reading.device_id}")
              producer.send(new ProducerRecord[String, String](OutputTopic, reading.device_id, json))
              producer.flush()
            }
          case None =>
            println(s"[ERREUR] JSON malformé, message ignoré : $json")
        }
        consumer.commitSync()
      }
      loop()
    }

    loop()
  }
}
