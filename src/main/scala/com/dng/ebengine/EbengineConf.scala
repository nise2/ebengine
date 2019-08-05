package com.dng.ebengine

object EbengineConf {

  // Spark
  val SPARK_APP                     : String  = "Ebengine"
  val SPARK_MASTER                  : String  = "local[*]"
  val SPARK_LOG_LEVEL               : String  = "ERROR"

  // Log
  val LOG_APP_NAME                  : String  = "App name"
  val LOG_MASTER                    : String  = "Master"
  val LOG_DEPLOY_MODE               : String  = "Deploy mode"

  // Column names
  val COL_USER_ID                   : String  = "userId"
  val COL_ITEM_ID                   : String  = "itemId"
  val COL_RATING                    : String  = "rating"
  val COL_TIMESTAMP                 : String  = "timestamp"
  val COL_USER_ID_AS_INTEGER        : String  = "userIdAsInteger"

  // Input
  val INPUT_FORMAT                  : String  = "csv"

  // -- Tests
  val INPUT_DIR                     : String  = "src/main/resources/input/"
  val INPUT_TEST_FILE_100           : String  = "xag_100.csv"
  val INPUT_TEST_FILE_100_PATH      : String  = INPUT_DIR + INPUT_TEST_FILE_100
  
  // Output
  val OUTPUT_FILE_FORMAT            : String  = "com.databricks.spark.csv"
  val OUTPUT_DELIMITER_TOKEN        : String  = ","
  val OUTPUT_DIRECTORY              : String  = "src/test/resources/output/"

  // - LookupUser
  val OUTPUT_LOOKUP_USER_FILE       : String  = "lookup_users.csv"
  val OUTPUT_LOOKUP_USER_FILE_PATH  : String  = OUTPUT_DIRECTORY + OUTPUT_LOOKUP_USER_FILE

  val EXPECTED_LOOKUP_USERS_100_FILE: String  = "expected_lookup_users_100.csv"
  val EXPECTED_LOOKUP_USERS_100_PATH: String  = INPUT_DIR + EXPECTED_LOOKUP_USERS_100_FILE
  val EXPECTED_LOOKUPUSER_ROWS_100L : Long    = 98

  // - AggRatings
  val EXPECTED_MAX_TIMESTAMP        : Long    = 1476686557818L
}
