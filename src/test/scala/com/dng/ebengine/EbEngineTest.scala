package com.dng.ebengine

import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll

class EbengineTest() extends UnitTestContext with BeforeAndAfterAll {

  lazy val inputDF  : DataFrame = getInputDF()

  def getInputDF()  : DataFrame = {
    ss.read
      .format(EbengineConf.INPUT_FORMAT)
      .load(EbengineConf.INPUT_FILE)
      .toDF(EbengineConf.INPUT_COL_0,
        EbengineConf.INPUT_COL_1,
        EbengineConf.INPUT_COL_2,
        EbengineConf.INPUT_COL_3)
  }

  describe("The DataFrame") {

    describe("when comparing the timestamps") {

      it("should return the max value") {
        val scope = new AggRatings
        val method = PrivateMethod[Long]('getMaxTimestamp)
        val result = scope invokePrivate method(inputDF)

        assert(result == EbengineConf.EXPECTED_MAX_TIMESTAMP)
      }
    }
  }
}
