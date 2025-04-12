package com.adform.farm.revenue

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object FarmRevenueAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FarmRevenueAnalysis")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

//    data load

    val harvestDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("E:\\GitRepos\\spark-poc\\spark-poc\\src\\main\\resources\\harvest.csv")
      .select("gatherer","date","fruit","amount")

//    harvestDF.printSchema()
//    harvestDF.show(10,false)

    val pricestDF = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("E:\\GitRepos\\spark-poc\\spark-poc\\src\\main\\resources\\prices.csv")
      .select("fruit","date","price")

//    pricestDF.printSchema()
//    pricestDF.show(10, false)

//    --------------------------------------------------------------
//    build basic revenue (joining production and price) DF for reuse

    val gathererDataWithRevenueAndMonthDF = harvestDF
      .join(pricestDF, Seq("date", "fruit"), "inner")
      .select("gatherer", "date", "fruit", "amount", "price")
      .withColumn("revenue", col("amount") * col("price"))
      .withColumn("month", substring(col("date"), 0, 7))
      .select("gatherer", "date", "fruit", "amount", "price", "revenue", "month")

//    gathererDataWithRevenueAndMonthDF.show(10, false)

//    --------------------------------------------------------------

    //  TODO: Task-1
    //   1. Who is your best gatherer in terms of the amounts (weight) of fruits gathered every month

    println(s"\n\n\n1. Who is your best gatherer in terms of the amounts (weight) of fruits gathered every month")
    val bestGathererPerMonthDF = harvestDF
      .withColumn("month", substring(col("date"), 0, 7))
      .select("month","gatherer",  "fruit", "amount")
      .groupBy("month","gatherer")
      .agg(round(sum("amount"),2).alias("total_volume_collected_by_gatherer"))
      .select("month","gatherer","total_volume_collected_by_gatherer")
      .withColumn("rnk", row_number().over(Window.partitionBy("month").orderBy(col("total_volume_collected_by_gatherer").desc)))
      .filter(col("rnk")===1)
      .select("month","gatherer","total_volume_collected_by_gatherer")
      .sort("month")

    bestGathererPerMonthDF.show(12,false)

    //  TODO: Task-2
    //   2. Are there employees that are better at gathering some specific fruit?

    println(s"2. Are there employees that are better at gathering some specific fruit?")
    val gathererSpecialityForFruitDF = harvestDF
      .select("gatherer", "fruit", "amount")
      .groupBy("gatherer", "fruit")
      .agg(round(sum("amount"),2).alias("total_volume_collected"))
      .select("gatherer", "fruit", "total_volume_collected")
      .withColumn("rnk", row_number().over(Window.partitionBy("gatherer").orderBy(col("total_volume_collected").desc)))
      .filter(col("rnk") === 1)
      .select("gatherer", "fruit", "total_volume_collected")

    gathererSpecialityForFruitDF.show(100,false)


    //  TODO: Task-3 & Task-4
    //   First, Common Logic for Task-3 & Task-4
    val earningsOfFruitsByMonthDF = gathererDataWithRevenueAndMonthDF
      .select("month", "fruit", "revenue")
      .groupBy("month", "fruit")
      .agg(round(sum(col("revenue")),2).alias("revenue_of_fruit_for_month"))
      .select("month", "fruit", "revenue_of_fruit_for_month")

    val earningsOfFruitsOverallDF = gathererDataWithRevenueAndMonthDF
      .select("fruit", "revenue")
      .groupBy("fruit")
      .agg(round(sum(col("revenue")),2).alias("overall_revenue_of_fruit"))
      .select("fruit", "overall_revenue_of_fruit")


    //TODO: 3. What is your best-earning fruit (overall and by month)?
    // Best-earning fruit by Month

    println(s"3. What is your best-earning fruit by month?")
    val bestEarningFruitByMonthDF = earningsOfFruitsByMonthDF
      .withColumn("rnk", row_number().over(Window.partitionBy("month").orderBy(col("revenue_of_fruit_for_month").desc)))
      .filter(col("rnk") === 1)
      .select("month", "fruit", "revenue_of_fruit_for_month")

    bestEarningFruitByMonthDF.show(15, false)

    // Best-earning fruit overall
    println(s"3. What is your best-earning fruit overall?")
    val bestEarningFruitOverallDF = earningsOfFruitsOverallDF
      .withColumn("rnk", row_number().over(Window.orderBy(col("overall_revenue_of_fruit").desc)))
      .filter(col("rnk") === 1)
      .select("fruit", "overall_revenue_of_fruit")

    bestEarningFruitOverallDF.show(15, false)


    //  TODO: Task-4
    //   4. Which is your least profitable fruit (again, overall and by month)?
    //   Least-earning fruit by Month
    println(s"4. Which is your least profitable fruit by month?")
    val leastEarningFruitByMonthDF = earningsOfFruitsByMonthDF
      .withColumn("rnk", row_number().over(Window.partitionBy("month").orderBy(col("revenue_of_fruit_for_month").asc)))
      .filter(col("rnk") === 1)
      .select("month", "fruit", "revenue_of_fruit_for_month")

    leastEarningFruitByMonthDF.show(15, false)

    //   Least-earning fruit Overall
    println(s"4. Which is your least profitable fruit overall?")
    val leastEarningFruitOverallDF = earningsOfFruitsOverallDF
      .withColumn("rnk", row_number().over(Window.orderBy(col("overall_revenue_of_fruit").asc)))
      .filter(col("rnk") === 1)
      .select("fruit", "overall_revenue_of_fruit")

    leastEarningFruitOverallDF.show(15, false)

    //  TODO: Task-4
    //   5. Which gatherer contributed most to your income (during the whole year and by month)?
    //   best revenue gatherer per month
    println(s"5. Which gatherer contributed most to your income by month?")
    val bestRevenueGathererPerMonthDF = gathererDataWithRevenueAndMonthDF
      .select("gatherer", "month", "revenue")
      .groupBy("month", "gatherer")
      .agg(round(sum(col("revenue")),2).alias("revenue_by_gatherer_for_month"))
      .withColumn("gatherer_rank_for_month", row_number().over(Window.partitionBy("month").orderBy(col("revenue_by_gatherer_for_month").desc)))
      .filter(col("gatherer_rank_for_month") === 1)
      .select("month", "gatherer", "revenue_by_gatherer_for_month")
      .sort(col("month").asc)

    bestRevenueGathererPerMonthDF.show(12, false)

    //   best revenue gatherer overall
    println(s"5. Which gatherer contributed most to your income by overall?")
    val bestRevenueGathererOverallDF = gathererDataWithRevenueAndMonthDF
      .select("gatherer", "revenue")
      .groupBy("gatherer")
      .agg(round(sum(col("revenue")),2).alias("overall_revenue_by_gatherer"))
      .withColumn("rnk", row_number().over(Window.orderBy(col("overall_revenue_by_gatherer").desc)))
      .filter(col("rnk") === 1)
      .select("gatherer", "overall_revenue_by_gatherer")


    bestRevenueGathererOverallDF.show(12, false)

    spark.stop()
  }

}
