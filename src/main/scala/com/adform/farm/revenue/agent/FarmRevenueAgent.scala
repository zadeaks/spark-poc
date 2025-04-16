package com.adform.farm.revenue.agent

import com.adform.farm.revenue.builder.RevenueBuilder
import com.adform.farm.revenue.utils.{DataFrameLoader, LogMaster, SparkContainer, StorageHandler}
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
object FarmRevenueAgent extends LogMaster {

  def run(revenueBuilder: RevenueBuilder)(implicit spark: SparkSession): Unit = {


    val bestGathererPerMonth = revenueBuilder.bestGathererPerMonth
    StorageHandler.persistAsCsv(bestGathererPerMonth, "bestGathererPerMonth")

    val gathererSpecialityForFruit = revenueBuilder.gathererSpecialityForFruit
    StorageHandler.persistAsCsv(gathererSpecialityForFruit, "gathererSpecialityForFruit")

    val bestEarningFruitByMonth = revenueBuilder.bestEarningFruitByMonth
    StorageHandler.persistAsCsv(bestEarningFruitByMonth, "bestEarningFruitByMonth")

    val bestEarningFruitOverall = revenueBuilder.bestEarningFruitOverall
    StorageHandler.persistAsCsv(bestEarningFruitOverall, "bestEarningFruitOverall")

    val leastEarningFruitByMonth = revenueBuilder.leastEarningFruitByMonth
    StorageHandler.persistAsCsv(leastEarningFruitByMonth, "leastEarningFruitByMonth")

    val leastEarningFruitOverall = revenueBuilder.leastEarningFruitOverall
    StorageHandler.persistAsCsv(leastEarningFruitOverall, "leastEarningFruitOverall")

    val bestRevenueGathererPerMonth = revenueBuilder.bestRevenueGathererPerMonth
    StorageHandler.persistAsCsv(bestRevenueGathererPerMonth, "bestRevenueGathererPerMonth")

    val bestRevenueGathererOverall = revenueBuilder.bestRevenueGathererOverall
    StorageHandler.persistAsCsv(bestRevenueGathererOverall, "bestRevenueGathererOverall")
  }

  def main(args: Array[String]): Unit = {
    val env = "dev"
    implicit val spark: SparkSession = SparkContainer.initializeSparkContainer(env)

    val dataFrameLoader = DataFrameLoader()
    val revenueBuilder = RevenueBuilder(dataFrameLoader)

    run(revenueBuilder)

    logInfo(s"Run for FarmRevenue successfully completed on ${LocalDateTime.now().toString}")

    spark.stop

  }

}
