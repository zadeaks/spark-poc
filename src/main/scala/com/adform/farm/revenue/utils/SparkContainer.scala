package com.adform.farm.revenue.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object SparkContainer extends LogMaster {

  def initializeSparkContainer(env: String): SparkSession = {

    val sparkConf = getSparkConfig(env)

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("INFO")

    spark

  }

  def getSparkConfig(env: String): SparkConf = {

    val config = ConfigFactory.load(s"application_${env}.conf")
    val sparkConfig = config.getConfig("spark")

    val driverMemory = sparkConfig.hasPath("driver.memory") match {
      case true => sparkConfig.getString("driver.memory")
      case false => "2g"
    }

    val driverCores = sparkConfig.hasPath("driver.core") match {
      case true => sparkConfig.getString("driver.core")
      case false => "1"
    }

    val executorMemory = sparkConfig.hasPath("executor.memory") match {
      case true => sparkConfig.getString("executor.memory")
      case false => "4g"
    }
    val executorCores = sparkConfig.hasPath("executor.core") match {
      case true => sparkConfig.getString("executor.core")
      case false => "2"
    }
    val numExecutors = sparkConfig.hasPath("num.executor") match {
      case true => sparkConfig.getString("num.executor")
      case false => "1"
    }


    val conf: SparkConf = new SparkConf()
      .setAppName(sparkConfig.getString("appName"))
      .setMaster(sparkConfig.getString("master"))

    if (env == "prod") {
      conf
        .set("spark.driver.memory", driverMemory)
        .set("spark.driver.core", driverCores)
        .set("spark.executor.memory", executorMemory)
        .set("spark.executor.core", executorCores)
        .set("spark.num.executor", numExecutors)
    }
    conf
  }

}
