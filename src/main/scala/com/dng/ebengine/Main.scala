package com.dng.ebengine

import com.dng.ebengine.lookup.AggRatings
import com.dng.ebengine.utils.DataFrameUtils
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.{SparkSession}

/**
  * Entry object of the job.
  */
object Main {

  /**
    * Main tasks of the job.
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val ss = initContext
    executeJob(ss)
    endContext(ss)
  }

  /**
    * Execute the job
    * @param ss SparkSession
    */
  private def executeJob(implicit ss: SparkSession): Unit = {
    val aggRatings = new AggRatings
    val inputDF = DataFrameUtils.getInputDF(EbengineConf.INPUT_TEST_FILE_102_PATH)

    val aggRatingsDF = aggRatings.generateDF(inputDF)(ss)
    aggRatings.writeDFToFile(aggRatingsDF, EbengineConf.JOB_OUTPUT_AGGRATINGS_PATH)
  }

  /**
    * Initiate Spark (SparkSession, SparkConf, sqlContext)
    * @return SparkSession
    */
  private def initContext: SparkSession = {
    println(EbengineConf.START_JOB_MSG)
    val appName = EbengineConf.SPARK_APP
    val master = EbengineConf.SPARK_MASTER

    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    implicit val ss = SparkSession.builder().config(conf).getOrCreate()

    val sc = ss.sparkContext
    sc.setLogLevel(EbengineConf.SPARK_LOG_LEVEL)

    val sqlContext = ss.sqlContext

    println(EbengineConf.LOG_APP_NAME + " : " + sc.appName)
    println(EbengineConf.LOG_MASTER + " : " + sc.master)
    println(EbengineConf.LOG_DEPLOY_MODE + " : " + sc.deployMode)

    return ss
  }

  /**
    * End the job with cleaning tasks.
    * @param ss
    */
  private def endContext(implicit ss: SparkSession): Unit = {
    println(EbengineConf.END_JOB_MSG)
    ss.sparkContext.stop
  }
}
