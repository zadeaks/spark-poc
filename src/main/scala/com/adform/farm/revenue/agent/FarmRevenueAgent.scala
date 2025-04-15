package com.adform.farm.revenue.agent

import com.adform.farm.revenue.utils.{DataFrameLoader, SparkContainer, StorageHandler}
import com.adform.farm.revenue.builder.RevenueBuilder
import org.apache.spark.sql.SparkSession
object FarmRevenueAgent {

  def run(dataFrameLoader: DataFrameLoader)(implicit spark: SparkSession): Unit = {
    val bestGathererPerMonth = RevenueBuilder(dataFrameLoader).bestGathererPerMonth
    StorageHandler.persistAsCsv(bestGathererPerMonth,"bestGathererPerMonth")

    val gathererSpecialityForFruit = RevenueBuilder(dataFrameLoader).gathererSpecialityForFruit
    StorageHandler.persistAsCsv(gathererSpecialityForFruit,"gathererSpecialityForFruit")

    val bestEarningFruitByMonth = RevenueBuilder(dataFrameLoader).bestEarningFruitByMonth
    StorageHandler.persistAsCsv(bestEarningFruitByMonth,"bestEarningFruitByMonth")

    val bestEarningFruitOverall = RevenueBuilder(dataFrameLoader).bestEarningFruitOverall
    StorageHandler.persistAsCsv(bestEarningFruitOverall,"bestEarningFruitOverall")

    val leastEarningFruitByMonth = RevenueBuilder(dataFrameLoader).leastEarningFruitByMonth
    StorageHandler.persistAsCsv(leastEarningFruitByMonth,"leastEarningFruitByMonth")

    val leastEarningFruitOverall = RevenueBuilder(dataFrameLoader).leastEarningFruitOverall
    StorageHandler.persistAsCsv(leastEarningFruitOverall,"leastEarningFruitOverall")

    val bestRevenueGathererPerMonth = RevenueBuilder(dataFrameLoader).bestRevenueGathererPerMonth
    StorageHandler.persistAsCsv(bestRevenueGathererPerMonth,"bestRevenueGathererPerMonth")

    val bestRevenueGathererOverall = RevenueBuilder(dataFrameLoader).bestRevenueGathererOverall
    StorageHandler.persistAsCsv(bestRevenueGathererOverall,"bestRevenueGathererOverall")
  }

  def main(args: Array[String]): Unit = {
    val env = "dev"
    implicit val spark = SparkContainer.initializeSparkContainer(env)

    run(DataFrameLoader())

    spark.stop

  }

}
