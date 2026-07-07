class ConsumerNotifSpec extends munit.FunSuite {

  val reading: Reading = Reading("device_001", 1714123456L, 35, 342, 48.7, 2.4)

  test("buildEmailBody contains the essential alert information") {
    val body = ConsumerNotif.buildEmailBody(reading)
    assert(body.contains("Device ID  : device_001"))
    assert(body.contains("BPM        : 35"))
    assert(body.contains("lat=48.7, lng=2.4"))
    assert("""Horodatage : \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}""".r.findFirstIn(body).isDefined)
  }
}
