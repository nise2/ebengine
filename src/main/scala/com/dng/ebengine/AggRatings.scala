package com.dng.ebengine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.max

class AggRatings {

  def getMaxTimestamp(df: DataFrame): Long = {
    val maxVal = df.agg(max(EbengineConf.INPUT_COL_3)).head()
    maxVal.getString(0).toLong
  }
}
