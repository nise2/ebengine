package com.dng.ebengine

import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll

class AggRatingsTest()  extends UnitTestContext with BeforeAndAfterAll {

  lazy val scope      : AggRatings  = new AggRatings
  lazy val inputDF    : DataFrame   = getInputDF(EbengineConf.INPUT_TEST_FILE_100_PATH)

  def getInputDF(filePath: String) : DataFrame = {
    ss.read
      .format(EbengineConf.INPUT_FORMAT)
      .load(filePath)
      .toDF(EbengineConf.COL_USER_ID,
        EbengineConf.COL_ITEM_ID,
        EbengineConf.COL_RATING,
        EbengineConf.COL_TIMESTAMP)
  }

  describe("When giving " + EbengineConf.INPUT_TEST_FILE_100_PATH) {
    it("should return the max timestamp value") {
      val method = PrivateMethod[Long]('getMaxTimestamp)
      val result = scope invokePrivate method(inputDF)

      assert(result == EbengineConf.EXPECTED_MAX_TIMESTAMP)
    }
  }
}
