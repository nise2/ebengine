package com.dng.ebengine

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll

class LookupItemTest extends UnitTestContext with BeforeAndAfterAll {

  lazy val scope    : LookupItem  = new LookupItem
  lazy val inputDF  : DataFrame   = getInputDF(EbengineConf.INPUT_TEST_FILE_100_PATH)

  def getInputDF(filePath: String)  : DataFrame   = {
    ss.read
      .format(EbengineConf.INPUT_FORMAT)
      .load(filePath)
      .toDF(EbengineConf.COL_USER_ID,
        EbengineConf.COL_ITEM_ID,
        EbengineConf.COL_RATING,
        EbengineConf.COL_TIMESTAMP)
  }

  describe("When giving " + EbengineConf.INPUT_TEST_FILE_100) {
    it("should output the expected file content") {
      val method = PrivateMethod[DataFrame](EbengineConfTest.generateDFFun)
      val result = scope invokePrivate method(inputDF)

      val expectedResult = Util.convertCsvToDF(ss, EbengineConf.EXPEC_LOOKUP_ITEM_100_PATH)

      assert(result.except(expectedResult).toDF().count == 0)
    }

    it("should output expected file") {
      val method = PrivateMethod[DataFrame](EbengineConfTest.writeDFToFileFun)
      val result = scope invokePrivate method(inputDF, EbengineConf.OUTPUT_LOOKUP_ITEM_FILE_PATH)

      assert(true,
        Files.exists(Paths.get(EbengineConf.OUTPUT_LOOKUP_ITEM_FILE_PATH)))
    }

  }

}
