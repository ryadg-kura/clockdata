import jakarta.mail._
import jakarta.mail.internet.{InternetAddress, MimeMessage}
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.{Duration, Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.jdk.CollectionConverters._

object ConsumerNotif {

  val AlertTopic: String = "alerts"

  private val DateFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault())

  /** Construit le corps de l'email à partir d'une mesure d'alerte. */
  def buildEmailBody(reading: Reading): String = {
    val ts = DateFormatter.format(Instant.ofEpochSecond(reading.timestamp))
    s"""ALERTE CARDIAQUE DÉTECTÉE
       |
       |Device ID  : ${reading.device_id}
       |BPM        : ${reading.bpm}
       |Horodatage : $ts
       |Position   : lat=${reading.lat}, lng=${reading.lng}
       |
       |Action immédiate requise.""".stripMargin
  }

  /** Envoie l'email d'alerte via SMTP (STARTTLS). */
  def sendAlertEmail(reading: Reading): Unit = {
    val smtpHost = sys.env.getOrElse("SMTP_HOST", "smtp.gmail.com")
    val smtpPort = sys.env.getOrElse("SMTP_PORT", "587")
    val smtpUser = sys.env("SMTP_USER")
    val smtpPassword = sys.env("SMTP_PASSWORD")
    val recipient = sys.env("ALERT_RECIPIENT")

    val props = new Properties()
    props.put("mail.smtp.host", smtpHost)
    props.put("mail.smtp.port", smtpPort)
    props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.starttls.enable", "true")

    val session = Session.getInstance(
      props,
      new Authenticator {
        override def getPasswordAuthentication: PasswordAuthentication =
          new PasswordAuthentication(smtpUser, smtpPassword)
      }
    )

    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(smtpUser))
    message.setRecipients(Message.RecipientType.TO, recipient)
    message.setSubject(s"[ClockData] ALERTE BPM critique — device ${reading.device_id}")
    message.setText(buildEmailBody(reading))

    Transport.send(message)
    println(s"[EMAIL] Alerte envoyée à $recipient pour device ${reading.device_id}")
  }

  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")

    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", bootstrap)
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("group.id", "alert-notifier")
    consumerProps.put("auto.offset.reset", "latest")
    consumerProps.put("enable.auto.commit", "false")

    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(java.util.List.of(AlertTopic))

    println(s"[ConsumerNotif] En écoute sur le topic '$AlertTopic'...")

    while (true) {
      val records = consumer.poll(Duration.ofMillis(1000))
      for (record <- records.asScala) {
        Reading.parse(record.value()) match {
          case Some(reading) =>
            try {
              sendAlertEmail(reading)
            } catch {
              case e: Exception =>
                println(s"[ERREUR] Envoi email échoué : ${e.getMessage}")
            } finally {
              consumer.commitSync()
            }
          case None =>
            println(s"[ERREUR] JSON malformé, message ignoré : ${record.value()}")
            consumer.commitSync()
        }
      }
    }
  }
}
