import jakarta.mail._
import jakarta.mail.internet.{InternetAddress, MimeMessage}
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.{Duration, Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

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

    // jakarta.mail n'accepte que java.util.Properties (pas de surcharge Map) : on construit
    // la config en Map immuable, puis on ne mute qu'à cette unique frontière avec la librairie.
    val smtpConfig = Map(
      "mail.smtp.host" -> smtpHost,
      "mail.smtp.port" -> smtpPort,
      "mail.smtp.auth" -> "true",
      "mail.smtp.starttls.enable" -> "true"
    )
    val props = new java.util.Properties()
    props.putAll(smtpConfig.asJava)

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

    val consumerConfig = Map[String, AnyRef](
      "bootstrap.servers" -> bootstrap,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "alert-notifier",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )

    val consumer = new KafkaConsumer[String, String](consumerConfig.asJava)
    consumer.subscribe(java.util.List.of(AlertTopic))

    println(s"[ConsumerNotif] En écoute sur le topic '$AlertTopic'...")

    @tailrec
    def loop(): Unit = {
      val records = consumer.poll(Duration.ofMillis(1000))
      records.asScala.foreach { record =>
        Reading.parse(record.value()) match {
          case Some(reading) =>
            Try(sendAlertEmail(reading)) match {
              case Success(_) => ()
              case Failure(e) =>
                println(s"[ERREUR] Envoi email échoué : ${e.getMessage}")
            }
            consumer.commitSync()
          case None =>
            println(s"[ERREUR] JSON malformé, message ignoré : ${record.value()}")
            consumer.commitSync()
        }
      }
      loop()
    }

    loop()
  }
}
