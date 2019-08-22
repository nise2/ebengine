package com.dng.ebengine

import org.apache.spark.sql.{DataFrame}

abstract class ALookup extends Serializable {

  def generateDF(df: DataFrame): DataFrame
  def writeDFToFile(df: DataFrame, outputPath: String): Unit

}
