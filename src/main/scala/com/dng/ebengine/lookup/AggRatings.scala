package com.dng.ebengine.lookup

import com.dng.ebengine.EbengineConf
import com.dng.ebengine.utils._
import org.apache.spark.sql.functions.{col, max, udf, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Manage AggRatings.
  */
class AggRatings extends ALookup {
  private val lookupItem: LookupItem = new LookupItem()
  private val lookupUser: LookupUser = new LookupUser()
  var maxTimestamp: Long = 0L

  /**
    * Generate AggRatings DataFrame with the following columns:
    * userIdAsInteger, itemIdAsInteger, ratingSum
    * @param inputDF Loaded DataFrame input
    * @param ss Spark Session
    * @return AggRatings DataFrame
    */
  def generateDF(inputDF: DataFrame)(implicit ss: SparkSession): DataFrame = {
    maxTimestamp  = getMaxTimestamp(inputDF)

    val userDF = lookupUser.generateDF(inputDF)
    val itemDF = lookupItem.generateDF(inputDF)
    lookupUser.writeDFToFile(userDF, EbengineConf.JOB_OUTPUT_LOOKUP_USER_PATH)
    lookupItem.writeDFToFile(itemDF, EbengineConf.JOB_OUTPUT_LOOKUP_ITEM_PATH)

    val fullInputDF = joinAllDataFrames(inputDF, userDF, itemDF)
    val ratedPenaltyDF = getDFWithRatingPenalty(fullInputDF)
    val sumRatedDF = getDFWithRatingPenaltySum(ratedPenaltyDF)
    val aggRatingsDF = filterSumRatedDF(sumRatedDF)

    aggRatingsDF
  }

  /**
    *
    * @param sumRatedDF
    * @return
    */
  private def filterSumRatedDF(sumRatedDF: DataFrame): Dataset[Row] = {
    sumRatedDF.filter(col(EbengineConf.COL_RATING_SUM) > EbengineConf.FILTER_PENALTY_SUM)
  }

  /**
    *
    * @param ratedPenaltyDF
    * @return
    */
  private def getDFWithRatingPenaltySum(ratedPenaltyDF: DataFrame): DataFrame = {
    ratedPenaltyDF
      .groupBy(col(EbengineConf.COL_USER_ID_AS_INTEGER), col(EbengineConf.COL_ITEM_ID_AS_INTEGER ))
      .agg(sum(EbengineConf.COL_RATING))
      .withColumnRenamed("sum("+ EbengineConf.COL_RATING +")",
        EbengineConf.COL_RATING_SUM)
      .withColumn(EbengineConf.COL_RATING_SUM, FormatterUtils.formatFloat(col(EbengineConf.COL_RATING_SUM)))
  }

  /**
    *
    * @param fullInputDF
    * @return The DataFrame with an update of the ratings
    *         (mallus applied for each number of day of difference with the maxTimestamp).
    */
  private def getDFWithRatingPenalty(fullInputDF: DataFrame): DataFrame = {
    fullInputDF
      .withColumn(EbengineConf.COL_RATING,
        applyRatingPenaltyUDF(col(EbengineConf.COL_TIMESTAMP), col(EbengineConf.COL_RATING)))
  }

  /**
    * Get the maxTimestamp of a DataFrame
    * @param df The DataFrame to search in.
    * @return The most recent timestamp of the DataFrame.
    */
  private def getMaxTimestamp(df: DataFrame): Long = {
    val maxVal = df.agg(max(EbengineConf.COL_TIMESTAMP)).head()
    maxVal.getString(0).toLong
  }

  /**
    * Create a DataFrame with all the input information for AggRatings.
    * @param inputDF DataFrame from the input file.
    * @param userDF LookupUser DataFrame.
    * @param itemDF LookupUser DataFrame.
    * @return A combined DataFrame with inputDF, userDF and itemDF.
    */
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

  /**
    * UserDefinedFunction : Update the rating with mallus for each number of day of difference with maxTimestamp.
    * (See applyRatingPenalty(maxTimestamp: Long, timestamp: Long, rating: Double): Double)
    */
  private val applyRatingPenaltyUDF = udf((timestamp: Long, rating: Double) =>  {
    val diffTimestamp: Long = maxTimestamp - timestamp

    val nbDays: Integer = getNbDayMsFromTimestamp(diffTimestamp)

    nbDays match {
      case x if x > 0 => getRatingPenalty(rating, nbDays)
      case _ => rating
    }
  })

  /**
    * Update the rating with mallus for each number of day of difference with maxTimestamp.
    * @param maxTimestamp The maxTimestamp of the input.
    * @param timestamp The timestamp of the current row.
    * @param rating The rating of the current row.
    * @return The new rating.
    */
  private def applyRatingPenalty(maxTimestamp: Long, timestamp: Long, rating: Double): Double = {
    val diffTimestamp: Long = maxTimestamp - timestamp
    val nbDays: Integer = getNbDayMsFromTimestamp(diffTimestamp)

    nbDays match {
      case x if x > 0 => getRatingPenalty(rating, nbDays)
      case _ => rating
    }
  }

  /**
    * Calculate the rating penalty of a rating.
    * @param rating Initial rating value.
    * @param nbDays Number of days of difference between its timestamp and maxTimestamp.
    * @return Get the mallused ratings based on a given numberOfDays.
    */
  private def getRatingPenalty(rating: Double, nbDays: Integer): Double = {
    rating * getPenaltyFactor(nbDays)
  }

  /**
    * Get the penalty factor to be applied to the rating.
    * @param nbDays Difference of days between its timestamp and maxTimestamp.
    * @return The penalty factor.
    */
  private def getPenaltyFactor(nbDays: Integer): Double = {
    Math.pow(EbengineConf.PENALTY_FACTOR, nbDays.toDouble)
  }

  /**
    * Get the number of day in milliseconds.
    * @param timestamp Timestamp that will be compared with the maxTimestamp.
    * @return The number of day in milliseconds.
    */
  private def getNbDayMsFromTimestamp(timestamp: Long): Integer = {
    (timestamp / EbengineConf.MS_TO_DAY).toInt
  }
}
