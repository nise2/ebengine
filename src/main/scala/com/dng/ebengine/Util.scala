package com.dng.ebengine

import org.apache.spark.sql.{DataFrame, SparkSession}

object Util {

  def writeIntoFile(df: DataFrame, filePath: String, coalesce: Integer = 1): Unit = {
    df.coalesce(coalesce)
      .write
      .option("header", true)
      .option("delimiter", EbengineConf.OUTPUT_DELIMITER_TOKEN)
      .format(EbengineConf.OUTPUT_FILE_FORMAT)
      .save(filePath)
  }

  def convertCsvToDF(ss: SparkSession, inputPath: String): DataFrame = {
    ss.read
      .option("header", true)
      .option("inferSchema", true)
      .format(EbengineConf.INPUT_FORMAT)
      .load(inputPath)
  }

  def getInputDF(ss: SparkSession, filePath: String)  : DataFrame = {
    ss.read
      .format(EbengineConf.INPUT_FORMAT)
      .load(filePath)
      .toDF(EbengineConf.COL_USER_ID,
        EbengineConf.COL_ITEM_ID,
        EbengineConf.COL_RATING,
        EbengineConf.COL_TIMESTAMP)
  }

}
