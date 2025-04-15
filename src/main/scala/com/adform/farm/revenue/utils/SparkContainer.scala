package com.adform.farm.revenue.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object SparkContainer {

  def initializeSparkContainer(env: String): SparkSession = {

    val sparkConf = getSparkConfig(env)

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    spark

  }

  def getSparkConfig(env: String): SparkConf = {


//    System.setProperty("env", env)
    val config = ConfigFactory.load(s"application_${env}.conf")
    val sparkConfig = config.getConfig("spark")

    val conf: SparkConf = new SparkConf()
      .setAppName(sparkConfig.getString("appName"))
      .setMaster(sparkConfig.getString("master"))

    if (env == "prod") {
      conf
        .set("spark.driver.memory", sparkConfig.getString("driver.memory"))
        .set("spark.driver.core", sparkConfig.getString("driver.core"))
        .set("spark.executor.memory", sparkConfig.getString("executor.memory"))
        .set("spark.executor.core", sparkConfig.getString("executor.core"))
        .set("spark.num.executor", sparkConfig.getString("num.executor"))
    }
    conf
  }

}
