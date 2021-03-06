package com.dng.ebengine

/**
  * Job configuration variables
  */
case object EbengineConf {

  /**
    * Spark
    */
  val SPARK_APP                     : String  = "Ebengine"
  val SPARK_MASTER                  : String  = "local[*]"
  val SPARK_LOG_LEVEL               : String  = "ERROR"
  val SPARK_PARAM_APP               : String  = "spark.app.name"
  val SPARK_PARAM_MASTER            : String  = "spark.master"
  val SPARK_PARAM_DEPLOY_MODE       : String  = "spark.submit.deployMode"

  /**
    * Log
    */
  val LOG_APP_NAME                  : String  = "App name"
  val LOG_MASTER                    : String  = "Master"
  val LOG_DEPLOY_MODE               : String  = "Deploy mode"
  val START_JOB_MSG                 : String  = "Ebengine start..."
  val END_JOB_MSG                   : String  = "Ebengine end..."
  val GENERATE_DATAFRAME_MSG        : String  = "Generate DataFrame into "

  /**
    * Math constants
    */
  val MS_TO_DAY                     : Long    = 86400000L
  val PENALTY_FACTOR                : Double  = 0.95
  val FILTER_PENALTY_SUM            : Float   = 0.01f

  /**
    * DataFrame and Table columns names
    */
  val COL_USER_ID                   : String  = "userId"
  val COL_ITEM_ID                   : String  = "itemId"
  val COL_RATING                    : String  = "rating"
  val COL_TIMESTAMP                 : String  = "timestamp"
  val COL_USER_ID_AS_INTEGER        : String  = "userIdAsInteger"
  val COL_ITEM_ID_AS_INTEGER        : String  = "itemIdAsInteger"
  val COL_RATING_SUM                : String  = "ratingSum"

  /**
    * Input
    */
  val RESOURCE_DIR                  : String  = "src/main/resources/input/"
  val INPUT_FORMAT                  : String  = "csv"
  val INPUT_TEST_FILE_100           : String  = "xag_100.csv"
  val INPUT_TEST_FILE_102           : String  = "xag_102.csv"

  /**
    * Output
    */
  val JOB_OUTPUT_DIR                : String  = "src/main/resources/output/"
  val OUTPUT_FILE_FORMAT            : String  = "com.databricks.spark.csv"
  val OUTPUT_DELIMITER_TOKEN        : String  = ","

  /**
    * Output files
    */
  val OUTPUT_LOOKUP_USER_FILE       : String  = "lookup_user.csv"
  val OUTPUT_LOOKUP_ITEM_FILE       : String  = "lookup_product.csv"
  val OUTPUT_AGGRATINGS_FILE        : String  = "agg_ratings.csv"


  /**
    * Predefined macros
    */
  val JOB_OUTPUT_LOOKUP_USER_PATH   : String  = JOB_OUTPUT_DIR + OUTPUT_LOOKUP_USER_FILE
  val JOB_OUTPUT_LOOKUP_ITEM_PATH   : String  = JOB_OUTPUT_DIR + OUTPUT_LOOKUP_ITEM_FILE
  val JOB_OUTPUT_AGGRATINGS_PATH    : String  = JOB_OUTPUT_DIR + OUTPUT_AGGRATINGS_FILE

  val INPUT_TEST_FILE_100_PATH      : String  = RESOURCE_DIR + INPUT_TEST_FILE_100
  val INPUT_TEST_FILE_102_PATH      : String  = RESOURCE_DIR + INPUT_TEST_FILE_102

  val OUTPUT_LOOKUP_USER_FILE_PATH  : String  = JOB_OUTPUT_DIR + OUTPUT_LOOKUP_USER_FILE
  val OUTPUT_LOOKUP_ITEM_FILE_PATH  : String  = JOB_OUTPUT_DIR + OUTPUT_LOOKUP_ITEM_FILE

  /**
    * Tables
    */
  val TB_COL_TOKEN                  : String  = "."
  val TB_INPUT                      : String  = "inputTable"
  val TB_INPUT_COL_USER_ID          : String  =  TB_INPUT + TB_COL_TOKEN + COL_USER_ID
  val TB_INPUT_COL_ITEM_ID          : String  =  TB_INPUT + TB_COL_TOKEN + COL_ITEM_ID
  val TB_USER                       : String  = "userTable"
  val TB_USER_COL_USER_ID           : String  =  TB_USER + TB_COL_TOKEN + COL_USER_ID
  val TB_USER_COL_ITEM_ID           : String  =  TB_USER + TB_COL_TOKEN + COL_ITEM_ID
  val TB_ITEM                       : String  = "itemTable"
  val TB_ITEM_COL_ITEM_ID           : String  =  TB_ITEM + TB_COL_TOKEN + COL_ITEM_ID
}
