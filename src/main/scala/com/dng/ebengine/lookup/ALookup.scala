package com.dng.ebengine.lookup

import com.dng.ebengine.EbengineConf
import com.dng.ebengine.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Abstract class for Lookup classes with common and specific DataFrame operations.
  */
abstract class ALookup extends Serializable {

  /**
    * Generate specific output DataFrame from an inputDataFrame.
    * @param df Loaded DataFrame input
    * @param ss Spark Session
    * @return Output DataFrame specific to implementation
    */
  def generateDF(df: DataFrame)(implicit ss: SparkSession): DataFrame

  /**
    * Write a DataFrame into in a specified output path.
    * @param df DataFrame to be written
    * @param outputPath Output location of the DataFrame
    * @param ss Spark Session
    */
  def writeDFToFile(df: DataFrame, outputPath: String)(implicit ss: SparkSession): Unit = {
    println(EbengineConf.GENERATE_DATAFRAME_MSG + outputPath)
    DataFrameUtils.writeIntoFile(df, outputPath)
  }
}
