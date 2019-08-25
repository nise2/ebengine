package com.dng.ebengine.lookup

import com.dng.ebengine.EbengineConf
import com.dng.ebengine.utils.DataFrameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class ALookup extends Serializable {

  def generateDF(df: DataFrame)(implicit ss: SparkSession): DataFrame

  def writeDFToFile(df: DataFrame, outputPath: String)(implicit ss: SparkSession): Unit = {
    println(EbengineConf.GENERATE_DATAFRAME_MSG + outputPath)
    DataFrameUtils.writeIntoFile(df, outputPath)
  }
}
