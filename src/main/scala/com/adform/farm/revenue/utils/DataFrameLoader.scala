package com.adform.farm.revenue.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameLoader(implicit val spark: SparkSession) {

  private def loadCsv(filepath: String)(implicit spark: SparkSession): DataFrame =
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filepath)

  val getHarvestDF: DataFrame = loadCsv("E:\\GitRepos\\spark-poc\\spark-poc\\src\\main\\resources\\data\\harvest.csv")
  val getPricesDF: DataFrame = loadCsv("E:\\GitRepos\\spark-poc\\spark-poc\\src\\main\\resources\\data\\prices.csv")

}

object DataFrameLoader {
  def apply()(implicit spark: SparkSession): DataFrameLoader = new DataFrameLoader()(spark)
}
