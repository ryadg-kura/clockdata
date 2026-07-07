import upickle.default._

import scala.util.Try

/** Mesure d'une montre connectée, telle que publiée dans le topic Kafka "clockdata". */
case class Reading(
    device_id: String,
    timestamp: Long,
    bpm: Int,
    steps: Int,
    lat: Double,
    lng: Double
)

object Reading {
  implicit val rw: ReadWriter[Reading] = macroRW

  /** Parse une mesure JSON. Retourne None si le JSON est malformé ou incomplet. */
  def parse(json: String): Option[Reading] = Try(read[Reading](json)).toOption
}
