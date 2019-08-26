package com.dng.ebengine.utils

import com.dng.ebengine.EbengineConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * DataFrameUtils
  */
object DataFrameUtils {

  /**
    * Save a DataFrame into a file with header and delimiter.
    * @param df DataFrame to be written.
    * @param filePath Output path where the DataFrame will be written.
    * @param coalesce Coalesce configuration value.
    */
  def writeIntoFile(df: DataFrame, filePath: String, coalesce: Integer = 1): Unit = {
    df.coalesce(coalesce)
      .write
      .option("header", true)
      .option("delimiter", EbengineConf.OUTPUT_DELIMITER_TOKEN)
      .format(EbengineConf.OUTPUT_FILE_FORMAT)
      .save(filePath)
  }

  /**
    * Load a DataFrame from Csv
    * @param ss SparkSession
    * @param inputPath Input file path
    * @return The DataFrame from the input csv
    */
  def convertCsvToDF(ss: SparkSession, inputPath: String): DataFrame = {
    ss.read
      .option("header", true)
      .option("inferSchema", true)
      .format(EbengineConf.INPUT_FORMAT)
      .load(inputPath)
  }

  /**
    * Get a DataFrame from the input file (.csv) send to the job.
    * @param filePath Path of the input file to be read.
    * @param ss SparkSession
    * @return Input (xag.csv typed) DataFrame with columns:
    * userId, itemId, rating, timestamp
    */
  def getInputDF(filePath: String)(implicit ss: SparkSession)  : DataFrame = {
    ss.read
      .format(EbengineConf.INPUT_FORMAT)
      .load(filePath)
      .toDF(EbengineConf.COL_USER_ID,
        EbengineConf.COL_ITEM_ID,
        EbengineConf.COL_RATING,
        EbengineConf.COL_TIMESTAMP)
  }
}
