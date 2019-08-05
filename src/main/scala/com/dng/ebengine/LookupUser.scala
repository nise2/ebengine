package com.dng.ebengine

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.monotonically_increasing_id

class LookupUser {

  def generateLookupUserDF(df: DataFrame): DataFrame = {
    // === Id starting from 1: DEPRECATED because may case OOM ===
    val newDF = df.select(EbengineConf.COL_USER_ID)
                      .distinct()
                      .sort(EbengineConf.COL_TIMESTAMP)
                      .withColumn(EbengineConf.COL_USER_ID_AS_INTEGER, monotonically_increasing_id())
                      .drop(EbengineConf.COL_ITEM_ID, EbengineConf.COL_TIMESTAMP)

    val windowSpec = Window.orderBy(EbengineConf.COL_USER_ID_AS_INTEGER)
    val idRows = functions.row_number().over(windowSpec) - 1

    newDF.withColumn(EbengineConf.COL_USER_ID_AS_INTEGER, idRows)
  }

  def writeLookupUserFile(df: DataFrame, outputPath: String): Unit = {
    val outputDF = generateLookupUserDF(df)
    Util.writeIntoFile(outputDF, outputPath)
  }
}
