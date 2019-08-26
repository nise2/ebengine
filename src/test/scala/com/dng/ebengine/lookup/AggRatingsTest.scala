package com.dng.ebengine.lookup

import com.dng.ebengine.{ContextUtils, EbengineConf, EbengineConfTestUtils}
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll

class AggRatingsTest  extends ContextUtils with BeforeAndAfterAll {

  lazy val scope      : AggRatings  = new AggRatings
  lazy val inputDF    : DataFrame   = getInputDF(EbengineConf.INPUT_TEST_FILE_102_PATH)

  def getInputDF(filePath: String) : DataFrame = {
    ss.read
      .format(EbengineConf.INPUT_FORMAT)
      .load(filePath)
      .toDF(EbengineConf.COL_USER_ID,
        EbengineConf.COL_ITEM_ID,
        EbengineConf.COL_RATING,
        EbengineConf.COL_TIMESTAMP)
  }

  it("should return the max timestamp value: " + EbengineConfTestUtils.EXPECTED_MAX_TIMESTAMP
    + " given " + EbengineConfTestUtils.INPUT_TEST_FILE_102_PATH) {
    // GIVEN
    val method = PrivateMethod[Long](EbengineConfTestUtils.getMaxTimestampFun)

    // WHEN
    val result = scope invokePrivate method(inputDF)

    // THEN
    assert(result == EbengineConfTestUtils.EXPECTED_MAX_TIMESTAMP_FULL)
  }

  it("should return the number of days in ms given a timestamp") {
    // GIVEN
    val method = PrivateMethod[Integer](EbengineConfTestUtils.getNbDayMsFromTimestamp)
    val timestamp = EbengineConfTestUtils.PARAM_TIMESTAMP

    // WHEN
    val result = scope invokePrivate method(timestamp)

    // THEN
    assert(result == EbengineConfTestUtils.RES_NB_DAY_MS_FROM_TS)
  }

  it ("should return the penalty factor given a rating and its gap timestamp with max") {
    // GIVEN
    val method = PrivateMethod[Double](EbengineConfTestUtils.getPenaltyFactorFun)
    val nbDay = EbengineConfTestUtils.PARAM_NB_DAY_SEVEN

    // WHEN
    val result: Double = scope invokePrivate method(nbDay)

    // THEN
    assert(result == EbengineConfTestUtils.RES_PENALTY_FACTOR_SEVEN_DAYS)
  }

  it ("should return the rating penalty given a rating and a number of days") {
    // GIVEN
    val method = PrivateMethod[Double](EbengineConfTestUtils.getRatingPenaltyFun)
    val rating = EbengineConfTestUtils.PARAM_RATING
    val nbDays = EbengineConfTestUtils.PARAM_NB_DAY_SEVEN

    // WHEN
    val result = scope invokePrivate method(rating, nbDays)

    // THEN
    assert(result == EbengineConfTestUtils.RES_RATING_BY_PENALTY_FACTOR_SEVEN_DAYS)
  }

  it("should return the rating without penalty given timestampMax - timestamp <= 0") {
    // GIVEN
    val method                = PrivateMethod[Double](EbengineConfTestUtils.applyRatingPenaltyFun)
    val maxTs       : Long    = EbengineConfTestUtils.PARAM_MAX_TS
    val tsNoPenalty : Long    = EbengineConfTestUtils.PARAM_TS_NO_PENALTY
    val rating      : Double  = EbengineConfTestUtils.PARAM_RATING

    // WHEN
    val result = scope invokePrivate method(maxTs, tsNoPenalty, rating)

    // THEN
    assert(result == EbengineConfTestUtils.PARAM_RATING)
  }

  it("should return the rating with penalty given timestampMax - timestamp > 0") {
    // GIVEN
    val method                = PrivateMethod[Double](EbengineConfTestUtils.applyRatingPenaltyFun)
    val maxTs     : Long      = EbengineConfTestUtils.PARAM_MAX_TS
    val tsPenalty : Long      = EbengineConfTestUtils.PARAM_TS_PENALTY
    val rating    : Double    = EbengineConfTestUtils.PARAM_RATING

    // WHEN
    val result = scope invokePrivate method(maxTs, tsPenalty, rating)

    // THEN
    assert(result == EbengineConfTestUtils.RES_RATING_PENALTY)
  }
}
