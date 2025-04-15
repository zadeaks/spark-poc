package com.adform.farm.revenue.utils

import org.apache.spark.sql.DataFrame

object StorageHandler {

  val unixTimestamp: Long = System.currentTimeMillis() / 1000

  def persistAsCsv(df: DataFrame, fileName: String, numPartFiles: Int = 1) =
    df.write
      .format("csv")
      .option("header", true)
      .option("spark.sql.files.ignoreMissingFiles", true)
      .mode("overwrite") // (other options: append, ignore, errorIfExists)
      .save(s"/output/csv/${fileName}_${unixTimestamp}.csv")

  def persistAsParquet(df: DataFrame, fileName: String, numPartFiles: Int = 1) =
    df.coalesce(numPartFiles)
      .write
      .format("parquet")
      .mode("append") // (other options:overwrite, append, ignore, errorIfExists)
      .save(s"/output/parquet/${fileName}_${unixTimestamp}.csv")

  def persistAsTable(df: DataFrame, tableName: String, numPartFiles: Int = 1) =
    df.write
      .mode("append") // (other options: append, ignore, errorIfExists)
      .saveAsTable(s"${tableName}")

}
