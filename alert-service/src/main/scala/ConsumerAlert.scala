import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._

object ConsumerAlert {

  val InputTopic: String = "clockdata"
  val OutputTopic: String = "alerts"
  val CriticalBpmThreshold: Int = 40

  /** Retourne true si le BPM est critique (inférieur au seuil). */
  def shouldAlert(bpm: Int): Boolean = bpm < CriticalBpmThreshold

  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")

    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", bootstrap)
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("group.id", "alert-detector")
    consumerProps.put("auto.offset.reset", "latest")
    consumerProps.put("enable.auto.commit", "false")

    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", bootstrap)
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val consumer = new KafkaConsumer[String, String](consumerProps)
    val producer = new KafkaProducer[String, String](producerProps)
    consumer.subscribe(java.util.List.of(InputTopic))

    println(s"[ConsumerAlert] En écoute sur le topic '$InputTopic'...")

    while (true) {
      val records = consumer.poll(Duration.ofMillis(1000))
      for (record <- records.asScala) {
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
    }
  }
}
