package com.dng.ebengine

import com.dng.ebengine.EbengineConf.{RESOURCE_DIR,
                                      OUTPUT_LOOKUP_USER_FILE, OUTPUT_LOOKUP_ITEM_FILE}

object EbengineConfTestUtils {

  val INPUT_TEST_FILE_100           : String  = "xag_100.csv"
  val INPUT_TEST_FILE_101           : String  = "xag_101.csv"
  val INPUT_TEST_FILE_102           : String  = "xag_102.csv"

  val TEST_OUTPUT_DIR                    : String  = "src/test/resources/output/"
  val TEST_OUTPUT_LOOKUP_USER_FILE_PATH  : String  = TEST_OUTPUT_DIR + OUTPUT_LOOKUP_USER_FILE
  val TEST_OUTPUT_LOOKUP_ITEM_FILE_PATH  : String  = TEST_OUTPUT_DIR + OUTPUT_LOOKUP_ITEM_FILE

  val EXPEC_LOOKUP_USER_100_FILE    : String  = "expected_lookup_user_100.csv"
  val EXPEC_LOOKUP_ITEM_100_FILE    : String  = "expected_lookup_product_100.csv"

  val INPUT_TEST_FILE_100_PATH      : String  = RESOURCE_DIR + INPUT_TEST_FILE_100
  val INPUT_TEST_FILE_101_PATH      : String  = RESOURCE_DIR + INPUT_TEST_FILE_101
  val INPUT_TEST_FILE_102_PATH      : String  = RESOURCE_DIR + INPUT_TEST_FILE_102
  val EXPEC_LOOKUP_USER_100_PATH    : String  = RESOURCE_DIR + EXPEC_LOOKUP_USER_100_FILE
  val EXPEC_LOOKUP_ITEM_100_PATH    : String  = RESOURCE_DIR + EXPEC_LOOKUP_ITEM_100_FILE


  // Tests
  val EXPECTED_MAX_TIMESTAMP_FULL   : Long    = 1476687768676L // lundi 17 octobre 2016 09:02:48.676 GMT+02:00 DST
  val EXPECTED_MAX_TIMESTAMP        : Long    = 1476686557818L // lundi 17 octobre 2016 08:42:37.818 GMT+02:00 DST

  // Test functions
  val generateDFFun                 : Symbol  = Symbol("generateDF")
  val writeDFToFileFun              : Symbol  = Symbol("writeDFToFile")

  val getMaxTimestampFun            : Symbol  = Symbol("getMaxTimestamp")

  val getNbDayMsFromTimestamp       : Symbol  = Symbol("getNbDayMsFromTimestamp")
  val PARAM_TIMESTAMP               : Long    = 1477353599713L
  val RES_NB_DAY_MS_FROM_TS         : Long    = 17098L

  val getPenaltyFactorFun           : Symbol  = Symbol("getPenaltyFactor")
  val PARAM_NB_DAY_SEVEN            : Integer = 7
  val RES_PENALTY_FACTOR_SEVEN_DAYS : Double  = 0.6983372960937497
  val RES_RATING_BY_PENALTY_FACTOR_SEVEN_DAYS  : Double  = 34.916864804687485

  val getRatingPenaltyFun           : Symbol  = Symbol("getRatingPenalty")
  val PARAM_MAX_TS                  : Long    = 1477353599713L
  val PARAM_TS_NO_PENALTY           : Long    = 1477353599713L
  val PARAM_TS_PENALTY              : Long    = PARAM_TS_NO_PENALTY - EbengineConf.MS_TO_DAY

  val applyRatingPenaltyFun         : Symbol  = Symbol("applyRatingPenalty")
  val PARAM_RATING                  : Double  = 50.0
  val RES_RATING_PENALTY            : Double  = 47.50
}
