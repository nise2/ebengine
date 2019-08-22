package com.dng.ebengine

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object Main {

  def main(args: Array[String]): Unit = {
    /* Init context */
    println("Ebengine start...")
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

    /* Execute main tasks */
    val aggRatings = new AggRatings
    aggRatings.generateDF


    /* Close context */

    println("Ebengine end...")
    sc.stop
  }
}
