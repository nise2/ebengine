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

  // Math
  val MS_TO_DAY                     : Long    = 86400000L
  val PENALTY_FACTOR                : Float   = 0f

  // Column names
  val COL_USER_ID                   : String  = "userId"
  val COL_ITEM_ID                   : String  = "itemId"
  val COL_RATING                    : String  = "rating"
  val COL_TIMESTAMP                 : String  = "timestamp"
  val COL_USER_ID_AS_INTEGER        : String  = "userIdAsInteger"
  val COL_ITEM_ID_AS_INTEGER        : String  = "itemIdAsInteger"

  // Input
  val RESOURCE_DIR                  : String  = "src/main/resources/input/"
  val INPUT_FORMAT                  : String  = "csv"
  // Input: Files
  val INPUT_TEST_FILE_100           : String  = "xag_100.csv"

  // Output: Conf
  val OUTPUT_DIRECTORY              : String  = "src/test/resources/output/"
  val OUTPUT_FILE_FORMAT            : String  = "com.databricks.spark.csv"
  val OUTPUT_DELIMITER_TOKEN        : String  = ","

  // Output: Files
  val OUTPUT_LOOKUP_USER_FILE       : String  = "lookup_user.csv"
  val OUTPUT_LOOKUP_ITEM_FILE       : String  = "lookup_product.csv"

  // Output: Expected files
  val EXPEC_LOOKUP_USER_100_FILE    : String  = "expected_lookup_user_100.csv"
  val EXPEC_LOOKUP_ITEM_100_FILE    : String  = "expected_lookup_product_100.csv"


  // Macro concatenation
  val INPUT_TEST_FILE_100_PATH      : String  = RESOURCE_DIR + INPUT_TEST_FILE_100

  val EXPEC_LOOKUP_USER_100_PATH    : String  = RESOURCE_DIR + EXPEC_LOOKUP_USER_100_FILE
  val EXPEC_LOOKUP_ITEM_100_PATH    : String  = RESOURCE_DIR + EXPEC_LOOKUP_ITEM_100_FILE

  val OUTPUT_LOOKUP_USER_FILE_PATH  : String  = OUTPUT_DIRECTORY + OUTPUT_LOOKUP_USER_FILE
  val OUTPUT_LOOKUP_ITEM_FILE_PATH  : String  = OUTPUT_DIRECTORY + OUTPUT_LOOKUP_ITEM_FILE

  // - AggRatings
  val EXPECTED_MAX_TIMESTAMP        : Long    = 1476686557818L
}
