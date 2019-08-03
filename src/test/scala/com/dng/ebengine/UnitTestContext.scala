package com.dng.ebengine

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec, PrivateMethodTester}

trait UnitTestContext extends FunSpec with BeforeAndAfterAll with PrivateMethodTester {
  var ss: SparkSession = _
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  override def beforeAll: Unit = {
    val appName = EbengineConf.SPARK_APP
    val master = EbengineConf.SPARK_MASTER

    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    ss = SparkSession.builder().config(conf).getOrCreate()

    sc = ss.sparkContext
    sqlContext = ss.sqlContext

    println("App name : " + sc.appName)
    println("Master : " + sc.master)
    println("Deploy mode : " + sc.deployMode)

    super.beforeAll
  }

  override def afterAll: Unit = {
    sc.stop
    super.afterAll
  }
}
