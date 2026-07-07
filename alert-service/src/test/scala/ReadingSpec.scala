class ReadingSpec extends munit.FunSuite {

  val validJson: String =
    """{"device_id":"device_001","timestamp":1714123456,"bpm":35,"steps":342,"lat":48.7,"lng":2.4}"""

  test("parse returns a Reading for valid JSON") {
    val result = Reading.parse(validJson)
    assertEquals(result, Some(Reading("device_001", 1714123456L, 35, 342, 48.7, 2.4)))
  }

  test("parse returns None for malformed JSON") {
    val result = Reading.parse("""{"device_id":"device_001", not valid json""")
    assertEquals(result, None)
  }

  test("parse returns None when a required field is missing") {
    val result = Reading.parse(
      """{"device_id":"device_001","timestamp":1714123456,"bpm":35,"steps":342,"lat":48.7}"""
    )
    assertEquals(result, None)
  }
}
