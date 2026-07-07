class ConsumerAlertSpec extends munit.FunSuite {

  test("shouldAlert is true for bpm strictly below 40") {
    assert(ConsumerAlert.shouldAlert(39))
    assert(ConsumerAlert.shouldAlert(25))
  }

  test("shouldAlert is false for bpm at or above 40") {
    assert(!ConsumerAlert.shouldAlert(40))
    assert(!ConsumerAlert.shouldAlert(72))
  }
}
