package com.dng.ebengine.lookup

import com.dng.ebengine.EbengineConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class LookupItem extends ALookup {

  def generateDF(df: DataFrame)(implicit ss: SparkSession): DataFrame = {

    val newDF = df.select(EbengineConf.COL_ITEM_ID)
      .distinct()
      .sort(EbengineConf.COL_TIMESTAMP)
      .withColumn(EbengineConf.COL_ITEM_ID_AS_INTEGER, monotonically_increasing_id())
      .drop(EbengineConf.COL_RATING, EbengineConf.COL_TIMESTAMP)

    val windowSpec = Window.orderBy(EbengineConf.COL_ITEM_ID_AS_INTEGER)
    val idRows = functions.row_number().over(windowSpec) - 1

    newDF.withColumn(EbengineConf.COL_ITEM_ID_AS_INTEGER, idRows)
  }
}
