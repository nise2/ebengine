package com.dng.ebengine.lookup

import com.dng.ebengine.EbengineConf
import com.dng.ebengine.utils._
import org.apache.spark.sql.functions.{col, max, udf, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class AggRatings extends ALookup {
  private val lookupItem: LookupItem = new LookupItem()
  private val lookupUser: LookupUser = new LookupUser()
  var maxTimestamp: Long = 0L

  def generateDF(inputDF: DataFrame)(implicit ss: SparkSession): DataFrame = {
    maxTimestamp  = getMaxTimestamp(inputDF)

    val userDF = lookupUser.generateDF(inputDF)
    val itemDF = lookupItem.generateDF(inputDF)
    lookupUser.writeDFToFile(userDF, EbengineConf.JOB_OUTPUT_LOOKUP_USER_PATH)
    lookupItem.writeDFToFile(itemDF, EbengineConf.JOB_OUTPUT_LOOKUP_ITEM_PATH)

    val fullInputDF = joinAllDataFrames(inputDF, userDF, itemDF)
    //fullInputDF.show()

    val ratedPenaltyDF = getDFWithRatingPenalty(fullInputDF)
    //ratedPenaltyDF.show()

    val sumRatedDF = getDFWithRatingPenaltySum(ratedPenaltyDF)
    //sumRatedDF.show()

    val aggRatingsDF = filterSumRatedDF(sumRatedDF)
    //aggRatingsDF.show()

    aggRatingsDF
  }

  private def filterSumRatedDF(sumRatedDF: DataFrame): Dataset[Row] = {
    sumRatedDF.filter(col(EbengineConf.COL_RATING_SUM) > EbengineConf.FILTER_PENALTY_SUM)
  }

  private def getDFWithRatingPenaltySum(ratedPenaltyDF: DataFrame): DataFrame = {
    ratedPenaltyDF
      .groupBy(col(EbengineConf.COL_USER_ID_AS_INTEGER), col(EbengineConf.COL_ITEM_ID_AS_INTEGER ))
      .agg(sum(EbengineConf.COL_RATING))
      .withColumnRenamed("sum("+ EbengineConf.COL_RATING +")",
        EbengineConf.COL_RATING_SUM)
      .withColumn(EbengineConf.COL_RATING_SUM, FormatterUtils.formatFloat(col(EbengineConf.COL_RATING_SUM)))
  }

  private def getDFWithRatingPenalty(fullInputDF: DataFrame): DataFrame = {
    fullInputDF
      .withColumn(EbengineConf.COL_RATING,
        applyRatingPenaltyUDF(col(EbengineConf.COL_TIMESTAMP), col(EbengineConf.COL_RATING)))
  }

  private def getMaxTimestamp(df: DataFrame): Long = {
    val maxVal = df.agg(max(EbengineConf.COL_TIMESTAMP)).head()
    maxVal.getString(0).toLong
  }

  private def joinAllDataFrames(inputDF: DataFrame,
                                userDF: DataFrame,
                                itemDF: DataFrame): Dataset[Row] = {
    val inputTable = inputDF.as(EbengineConf.TB_INPUT)
    val userTable = userDF.as(EbengineConf.TB_USER)
    val itemTable = itemDF.as(EbengineConf.TB_ITEM)

    inputTable
      .join(userTable, col(EbengineConf.TB_INPUT_COL_USER_ID ) === col(EbengineConf.TB_USER_COL_USER_ID))
      .join(itemTable, col(EbengineConf.TB_INPUT_COL_ITEM_ID) === col (EbengineConf.TB_ITEM_COL_ITEM_ID))
      .select(EbengineConf.COL_USER_ID_AS_INTEGER,
        EbengineConf.COL_ITEM_ID_AS_INTEGER,
        EbengineConf.COL_RATING,
        EbengineConf.COL_TIMESTAMP)
  }

  private val applyRatingPenaltyUDF = udf((timestamp: Long, rating: Double) =>  {
    val diffTimestamp: Long = maxTimestamp - timestamp

    val nbDays: Integer = getNbDayMsFromTimestamp(diffTimestamp)

    nbDays match {
      case x if x > 1 => getRatingPenalty(rating, nbDays)
      case _ => rating
    }
  })

  private def applyRatingPenalty(maxTimestamp: Long, timestamp: Long, rating: Double): Double = {
    val diffTimestamp: Long = maxTimestamp - timestamp
    val nbDays: Integer = getNbDayMsFromTimestamp(diffTimestamp)

    nbDays match {
      case x if x > 1 => getRatingPenalty(rating, nbDays)
      case _ => rating
    }
  }

  private def getRatingPenalty(rating: Double, nbDays: Integer): Double = {
    rating * getPenaltyFactor(nbDays)
  }

  private def getPenaltyFactor(nbDays: Integer): Double = {
    Math.pow(EbengineConf.PENALTY_FACTOR, nbDays.toDouble)
  }

  private def getNbDayMsFromTimestamp(timestamp: Long): Integer = {
    (timestamp / EbengineConf.MS_TO_DAY).toInt
  }
}
