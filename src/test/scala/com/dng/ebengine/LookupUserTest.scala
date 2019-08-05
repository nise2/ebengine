package com.dng.ebengine

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll

class LookupUserTest extends UnitTestContext with BeforeAndAfterAll {

  lazy val scope    : LookupUser  = new LookupUser
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
    it("should output expected file") {
      val method = PrivateMethod[DataFrame]('writeLookupUserFile)
      val result = scope invokePrivate method(inputDF, EbengineConf.OUTPUT_LOOKUP_USER_FILE_PATH)

      assert(true,
        Files.exists(Paths.get(EbengineConf.OUTPUT_LOOKUP_USER_FILE_PATH)))
    }

    it("should output the expected file content") {
      val method = PrivateMethod[DataFrame]('generateLookupUserDF)
      val result = scope invokePrivate method(inputDF)

      result.show(100, false)
      val expectedResult = Util.convertCsvToDF(ss, EbengineConf.EXPECTED_LOOKUP_USERS_100_PATH)
      expectedResult.show(100, false)

      assert(result.except(expectedResult).toDF().count == 0)
    }
  }
}
