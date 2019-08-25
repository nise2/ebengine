package com.dng.ebengine.lookup

import java.nio.file.{Files, Paths}

import com.dng.ebengine.utils.DataFrameUtils
import com.dng.ebengine.{ContextUtils, EbengineConf, EbengineConfTestUtils}
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll

class LookupUserTestUtils extends ContextUtils with BeforeAndAfterAll {

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

  it("should output the expected file content") {
    // GIVEN
    val result = scope.generateDF(inputDF)(ss)

    // WHEN
    val expectedResult = DataFrameUtils.convertCsvToDF(ss, EbengineConfTestUtils.EXPEC_LOOKUP_USER_100_PATH)

    // THEN
    assert(result.except(expectedResult).toDF().count == 0)
  }

  it("should output expected file") {
    // WHEN
    val result = scope.writeDFToFile(inputDF,
      EbengineConfTestUtils.TEST_OUTPUT_LOOKUP_USER_FILE_PATH)(ss)

    // THEN
    assert(true,
      Files.exists(Paths.get(EbengineConf.OUTPUT_LOOKUP_USER_FILE_PATH)))
  }
}
