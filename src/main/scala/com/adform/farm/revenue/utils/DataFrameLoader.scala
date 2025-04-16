package com.adform.farm.revenue.utils

import scala.util.{Try, Failure, Success}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameLoader(implicit val spark: SparkSession) extends LogMaster {

  private def loadCsv(filepath: String)(implicit spark: SparkSession): DataFrame = {
    Try {
      spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(filepath)
    } match {
      case Success(df) => logInfo(s"Successfully loaded file from path : $filepath")
        df
      case Failure(er) => logError(s"Failed to load data from $filepath", Some(er))
      throw er
    }
  }

  val getHarvestDF: DataFrame = loadCsv("E:\\GitRepos\\spark-poc\\spark-poc\\src\\main\\resources\\data\\harvest.csv")
  val getPricesDF: DataFrame = loadCsv("E:\\GitRepos\\spark-poc\\spark-poc\\src\\main\\resources\\data\\prices.csv")

}

object DataFrameLoader {
  def apply()(implicit spark: SparkSession): DataFrameLoader = new DataFrameLoader()(spark)
}
