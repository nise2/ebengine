package com.dng.ebengine

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.functions._


class AggRatings {

  var maxTimestamp: Long = 0L
  val lookupItem: LookupItem = new LookupItem()
  val lookupUser: LookupUser = new LookupUser()


  def generateDF(ss: SparkSession): DataFrame = {


    val inputDF = Util.getInputDF(ss, EbengineConf.INPUT_TEST_FILE_100_PATH)

    maxTimestamp = getMaxTimestamp(inputDF)

    println("maxTimestamp: " + maxTimestamp)

    val userDF = lookupUser.generateDF(inputDF)
    val itemDF = lookupItem.generateDF(inputDF)
    import ss.sqlContext.implicits._
    //val newDF = inputDF.as("inputDF").join(userDF.as("userDF")).join(itemDF.as("itemDF"))

    val inputTable = inputDF.as("inputTable")
    val userTable = userDF.as("userTable")
    val itemTable = itemDF.as("itemTable")

    //println(inputDF.count()) //100

    val mappedDF = inputTable
        .join(userTable, col("inputTable.userId") === col("userTable.userId"))
        .join(itemTable, col("inputTable.itemId") === col ("itemTable.itemId"))
        .select("userIdAsInteger", "itemIdAsInteger", "rating", "timestamp")

    val mappedTable = mappedDF.as("mappedTable")
    //println(newDF.count()) // 100

    ss.sqlContext.udf.register("getRatingPenaltyUDF")

    val ratedDF = mappedDF
      .withColumn("penalty", callUDF(getRatingPenalty("mappedTable.timestamp", "mappedTable.rating")))

    ratedDF.show(100)
    ratedDF
  }

  def getRatingPenalty(timestamp: Long, rating: Float): Float = {
    val diffTimestamp = maxTimestamp - timestamp
    val nbDays = getNbDaysfromTimestamp(diffTimestamp)

    rating match {
      case x if x > 1 => applyTimestampPenalty(rating)
      case _ => rating
    }
    println("rating: " + rating)
    rating
  }

  def applyTimestampPenalty(rating: Float): Float = {
    rating * EbengineConf.PENALTY_FACTOR
  }

  def getNbDaysfromTimestamp(timestamp: Long): Long = {
    val res = timestamp / EbengineConf.MS_TO_DAY
    println("Number of days: " + res)
    res
  }

  def getMaxTimestamp(df: DataFrame): Long = {
    val maxVal = df.agg(max(EbengineConf.COL_TIMESTAMP)).head()
    maxVal.getString(0).toLong
  }
}
