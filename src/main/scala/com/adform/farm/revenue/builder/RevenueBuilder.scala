package com.adform.farm.revenue.builder

import com.adform.farm.revenue.utils.DataFrameLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class RevenueBuilder(val dataframeLoader: DataFrameLoader)(implicit spark: SparkSession) {


  val harvestDF = dataframeLoader.getHarvestDF
    .select("gatherer", "date", "fruit", "amount")

  val pricesDF = dataframeLoader.getPricesDF
    .select("fruit", "date", "price")

  //    build basic revenue (joining production and price) DF for reuse
  val gathererDataWithRevenueAndMonthDF = harvestDF
    .join(pricesDF, Seq("date", "fruit"), "inner")
    .select("gatherer", "date", "fruit", "amount", "price")
    .withColumn("revenue", col("amount") * col("price"))
    .withColumn("month", substring(col("date"), 0, 7))
    .select("gatherer", "date", "fruit", "amount", "price", "revenue", "month")

  // calculate earnings of fruits by month and overall
  val earningsOfFruitsByMonthDF = gathererDataWithRevenueAndMonthDF
    .select("month", "fruit", "revenue")
    .groupBy("month", "fruit")
    .agg(round(sum(col("revenue")), 2).alias("revenue_of_fruit_for_month"))
    .select("month", "fruit", "revenue_of_fruit_for_month")

  val earningsOfFruitsOverallDF = gathererDataWithRevenueAndMonthDF
    .select("fruit", "revenue")
    .groupBy("fruit")
    .agg(round(sum(col("revenue")), 2).alias("overall_revenue_of_fruit"))
    .select("fruit", "overall_revenue_of_fruit")

  //   1. Who is your best gatherer in terms of the amounts (weight) of fruits gathered every month
  def bestGathererPerMonth: DataFrame = {
    val bestGathererPerMonthDF = harvestDF
      .withColumn("month", substring(col("date"), 0, 7))
      .select("month", "gatherer", "fruit", "amount")
      .groupBy("month", "gatherer")
      .agg(round(sum("amount"), 2).alias("total_volume_collected_by_gatherer"))
      .select("month", "gatherer", "total_volume_collected_by_gatherer")
      .withColumn("rnk", row_number().over(Window.partitionBy("month").orderBy(col("total_volume_collected_by_gatherer").desc)))
      .filter(col("rnk") === 1)
      .select("month", "gatherer", "total_volume_collected_by_gatherer")
      .sort("month")

    bestGathererPerMonthDF
  }

  //   2. Are there employees that are better at gathering some specific fruit?
  def gathererSpecialityForFruit: DataFrame =  {

    val gathererSpecialityForFruitDF = harvestDF
      .select("gatherer", "fruit", "amount")
      .groupBy("gatherer", "fruit")
      .agg(round(sum("amount"), 2).alias("total_volume_collected"))
      .select("gatherer", "fruit", "total_volume_collected")
      .withColumn("rnk", row_number().over(Window.partitionBy("gatherer").orderBy(col("total_volume_collected").desc)))
      .filter(col("rnk") === 1)
      .select("gatherer", "fruit", "total_volume_collected")

    gathererSpecialityForFruitDF
  }


  // 3. What is your best-earning fruit i) by month)?
  def bestEarningFruitByMonth: DataFrame = {
    val bestEarningFruitByMonthDF = earningsOfFruitsByMonthDF
      .withColumn("rnk", row_number().over(Window.partitionBy("month").orderBy(col("revenue_of_fruit_for_month").desc)))
      .filter(col("rnk") === 1)
      .select("month", "fruit", "revenue_of_fruit_for_month")

    bestEarningFruitByMonthDF
  }

  // 3. What is your best-earning fruit ii) by overall)?
  def bestEarningFruitOverall: DataFrame = {
    val bestEarningFruitOverallDF = earningsOfFruitsOverallDF
      .withColumn("rnk", row_number().over(Window.orderBy(col("overall_revenue_of_fruit").desc)))
      .filter(col("rnk") === 1)
      .select("fruit", "overall_revenue_of_fruit")

    bestEarningFruitOverallDF
  }

  // 4. What is your least-earning fruit i) by month)?
  def leastEarningFruitByMonth: DataFrame = {
    val leastEarningFruitByMonthDF = earningsOfFruitsByMonthDF
      .withColumn("rnk", row_number().over(Window.partitionBy("month").orderBy(col("revenue_of_fruit_for_month").asc)))
      .filter(col("rnk") === 1)
      .select("month", "fruit", "revenue_of_fruit_for_month")

    leastEarningFruitByMonthDF
  }
  // 4. What is your least-earning fruit ii) by overall)?
  def leastEarningFruitOverall: DataFrame = {
    val leastEarningFruitOverallDF = earningsOfFruitsOverallDF
      .withColumn("rnk", row_number().over(Window.orderBy(col("overall_revenue_of_fruit").asc)))
      .filter(col("rnk") === 1)
      .select("fruit", "overall_revenue_of_fruit")

    leastEarningFruitOverallDF
  }

//  5.best revenue gatherer i) Monthly
  def bestRevenueGathererPerMonth:DataFrame = {
    val bestRevenueGathererPerMonthDF = gathererDataWithRevenueAndMonthDF
      .select("gatherer", "month", "revenue")
      .groupBy("month", "gatherer")
      .agg(round(sum(col("revenue")), 2).alias("revenue_by_gatherer_for_month"))
      .withColumn("gatherer_rank_for_month", row_number().over(Window.partitionBy("month").orderBy(col("revenue_by_gatherer_for_month").desc)))
      .filter(col("gatherer_rank_for_month") === 1)
      .select("month", "gatherer", "revenue_by_gatherer_for_month")
      .sort(col("month").asc)

    bestRevenueGathererPerMonthDF

  }
//  5.best revenue gatherer ii) Overall
  def bestRevenueGathererOverall:DataFrame = {
    val bestRevenueGathererOverallDF = gathererDataWithRevenueAndMonthDF
      .select("gatherer", "revenue")
      .groupBy("gatherer")
      .agg(round(sum(col("revenue")), 2).alias("overall_revenue_by_gatherer"))
      .withColumn("rnk", row_number().over(Window.orderBy(col("overall_revenue_by_gatherer").desc)))
      .filter(col("rnk") === 1)
      .select("gatherer", "overall_revenue_by_gatherer")

    bestRevenueGathererOverallDF

  }


}

object RevenueBuilder {
  def apply(dataframeLoader: DataFrameLoader)(implicit spark: SparkSession): RevenueBuilder = new RevenueBuilder(dataframeLoader)(spark)
}
