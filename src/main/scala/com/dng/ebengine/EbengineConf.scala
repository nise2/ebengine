package com.dng.ebengine

object EbengineConf {
  val SPARK_APP     : String  = "Ebengine"
  val SPARK_MASTER  : String  = "local[*]"

  val INPUT_FILE    : String  = "src/test/resources/xag_100.csv"
  val INPUT_FORMAT  : String  = "csv"
  val INPUT_COL_0   : String  = "userId"
  val INPUT_COL_1   : String  = "itemId"
  val INPUT_COL_2   : String  = "rating"
  val INPUT_COL_3   : String  = "timestamp"

  val EXPECTED_MAX_TIMESTAMP
                    : Long    = 1476686557818L
}
