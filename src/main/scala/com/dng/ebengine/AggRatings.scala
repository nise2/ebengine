package com.dng.ebengine

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.functions._

import scala.math.BigDecimal.RoundingMode

class AggRatings extends Serializable {

  var maxTimestamp: Long = 0L
  val lookupItem: LookupItem = new LookupItem()
  val lookupUser: LookupUser = new LookupUser()

  def generateDF(implicit ss: SparkSession): DataFrame = {

    val inputDF = Util.getInputDF(ss, EbengineConf.INPUT_TEST_FILE_100_PATH)

    maxTimestamp = getMaxTimestamp(inputDF)

    val userDF = lookupUser.generateDF(inputDF)
    val itemDF = lookupItem.generateDF(inputDF)
    import ss.sqlContext.implicits._
    //val newDF = inputDF.as("inputDF").join(userDF.as("userDF")).join(itemDF.as("itemDF"))

    val inputTable = inputDF.as("inputTable")
    val userTable = userDF.as("userTable")
    val itemTable = itemDF.as("itemTable")

    //println("Before filter" + inputDF.count()) 11922520

    // 1. val timestampMsTable: Convert every values to ms

    // 2. a.GroupBy couple user & item
    // 2. b.Then aggregate with sum(rating)
    // Both a. & b. should output the same count dataframe value

    // 2. c. Should output the same count value as 2. a & 2. b

    // 3. a. Display only ratingSum >= 0.01
    // 3. b. The difference should output the same result

    val mappedDF = inputTable
        .join(userTable, col("inputTable.userId") === col("userTable.userId"))
        .join(itemTable, col("inputTable.itemId") === col ("itemTable.itemId"))
        .select("userIdAsInteger", "itemIdAsInteger", "rating", "timestamp")

    val mappedTable = mappedDF.as("mappedTable")

    mappedTable.show(200)

    def ratedDF = mappedDF
          .withColumn("rating",
          getRatingPenalty(col("timestamp"), col("rating"))
          )

    def sumRatedDF = ratedDF
        .groupBy(col("userIdAsInteger"), col("itemIdAsInteger"))
        .agg(sum("rating"))
        .withColumnRenamed("sum(rating)", "ratingSum")
        .withColumn("ratingSum", formatFloat(col("ratingSum")))
        .filter($"ratingSum" > 0.01)

    sumRatedDF
  }

  val formatFloat = udf((value: Float) => {
    val bd = BigDecimal(value)
    val formatted = bd.setScale(2, RoundingMode.HALF_EVEN)
    formatted.toFloat
  })

  val getRatingPenalty = udf((timestamp: Long, rating: Float) =>  {
    val diffTimestamp: Long = maxTimestamp - timestamp
    val nbDays: Long = getNbDaysfromTimestamp(diffTimestamp)

    nbDays match {
      case x if x > 1 => applyTimestampPenalty(rating, nbDays)
      case _ => rating
    }
  })

  def applyTimestampPenalty(rating: Float, nbDays: Long): Float = {
    val penaltyFactor = Math.pow(EbengineConf.PENALTY_FACTOR, nbDays)
    val res = rating * penaltyFactor
    res.toFloat
  }

  def getNbDaysfromTimestamp(timestamp: Long): Long = {
    timestamp / EbengineConf.MS_TO_DAY
  }

  def getMaxTimestamp(df: DataFrame): Long = {
    val maxVal = df.agg(max(EbengineConf.COL_TIMESTAMP)).head()
    maxVal.getString(0).toLong
  }
}
