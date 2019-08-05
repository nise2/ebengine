package com.dng.ebengine

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, PrivateMethodTester}

trait UnitTestContext extends FunSpec with BeforeAndAfterAll with PrivateMethodTester {
  var ss              : SparkSession  = _
  var sc              : SparkContext  = _
  var sqlContext      : SQLContext    = _

  override def beforeAll: Unit = {
    val appName = EbengineConf.SPARK_APP
    val master = EbengineConf.SPARK_MASTER

    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    ss = SparkSession.builder().config(conf).getOrCreate()

    sc = ss.sparkContext
    sc.setLogLevel(EbengineConf.SPARK_LOG_LEVEL)

    sqlContext = ss.sqlContext

    println(EbengineConf.LOG_APP_NAME     + " : " + sc.appName)
    println(EbengineConf.LOG_MASTER       + " : " + sc.master)
    println(EbengineConf.LOG_DEPLOY_MODE  + " : " + sc.deployMode)

    super.beforeAll
  }

  override def afterAll: Unit = {
    sc.stop
    super.afterAll
  }
}
