package com.harvest.software

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.desc

object HarvestMain extends App {

  // Setting up logger to reduce noise in output and only print Error, if any
  Logger.getLogger("org").setLevel(Level.ERROR)


  //Creating Spark session, master(local[all]). In case of running over cluster, input to be taken from spark-submit
  val spark = SparkSession
    .builder()
    .appName("Harvest App")
    .master("local[*]")
    .getOrCreate()

  // Reading harvest.csv to create dataframe out of it
  val harvestDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/Harvest.csv")

  // Reading price.csv to create dataframe out of it
  val priceDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/price.csv")

  /**
   *
   * Finding out Best Gatherer in terms of fruits gathered every month
   */
  val harvestDateFormatDF = harvestDF.withColumn("formatted_date", to_date(col("Date"), "MM/dd/yy"))
    .withColumn("month", month(col("formatted_date")))

  harvestDateFormatDF.createOrReplaceTempView("harvestTable")
  val sumDF = spark.sql("select Gatherer_name,month,sum(Quantity) as SumQty from harvestTable group by Gatherer_name, month")

  val maxHarvestDF = sumDF
    .withColumn("max_qty", dense_rank().over(Window.partitionBy("month").orderBy("SumQty")))
    .filter(col("max_qty").equalTo(1))
    .drop(col("max_qty"))

  println("Best Gatherer")
  maxHarvestDF.show()


  val countDF = spark.sql("select Gatherer_name,fruit, count(Fruit) as Count_Fruit from harvestTable group by Gatherer_name, fruit")
//  countDF.show()
  val BestGatherer = countDF
    .withColumn("max_qty", dense_rank().over(Window.partitionBy("fruit").orderBy("Count_fruit")))
    .filter(col("max_qty").equalTo(1))
    .drop(col("max_qty"))

  println("Finding if a gatherer is good at specific fruit")
  BestGatherer.show()

  val joinDF = harvestDF.join(priceDF, Seq("Date"), "left").drop(priceDF.col("fruit"))

  val earningDF = joinDF.withColumn("earning", col("Quantity") * col("Price"))

  earningDF.withColumn("formatted_date", to_date(col("Date"), "MM/dd/yy"))
    .withColumn("month", month(col("formatted_date")))
    .createOrReplaceTempView("EarningTable")

  val maxEarningDF = spark.sql("select fruit, sum(Earning) as Max_Earning from EarningTable group by fruit")
    .createOrReplaceTempView("maxEarningTable")

  /**
   * Profit from fruits each month and overall.
   * Max and Min profit each month wise.
   *
   */
  val HighProfitDF = spark.sql("select * from maxEarningTable order by max_earning desc limit 1")
  val LowProfitDF = spark.sql("select * from maxEarningTable order by max_earning limit 1")

  println("Overall Highest Profitable Fruit")
  HighProfitDF.show()

  println("Overall Least Profitable Fruit")
  LowProfitDF.show()

  earningDF.withColumn("formatted_date", to_date(col("Date"), "MM/dd/yy"))
    .withColumn("month", month(col("formatted_date")))
    .createOrReplaceTempView("EarningMonthWiseTable")

  val monthWiseEarningDF = spark.sql("select fruit,month, sum(Earning) as Max_Earning from EarningMonthWiseTable group by month,fruit")

  // Least and Max profitable fruit monthly
  val leastProfitMonthWise = monthWiseEarningDF
    .withColumn("max_qty", dense_rank().over(Window.partitionBy("month").orderBy("Max_Earning")))
    .filter(col("max_qty").equalTo(1))
    .drop(col("max_qty"))
  println("Monthly Least Profitable Fruit")
  leastProfitMonthWise.show()

  val MaxProfitMonthWise = monthWiseEarningDF
    .withColumn("max_qty", dense_rank().over(Window.partitionBy("month").orderBy(desc("Max_Earning"))))
    .filter(col("max_qty").equalTo(1))
    .drop(col("max_qty"))

  println("Monthly Highest Profitable Fruit")
  MaxProfitMonthWise.show()

  /**
   * Contribution of Gatherer each month and overall.
   * Max and Min contribution each month wise.
   *
   */

  val contribDF = spark.sql("select gatherer_name, sum(Earning) as Max_Earning from EarningTable group by gatherer_name")
    .createOrReplaceTempView("maxEarningTable")

  val highContribDF = spark.sql("select * from maxEarningTable order by max_earning desc limit 1")
  val lowContribDF = spark.sql("select * from maxEarningTable order by max_earning limit 1")


  println("Overall Highest Contrib of Gatherer")
  highContribDF.show()

  println("Overall Least Contrib of Gatherer")
  lowContribDF.show()

  val monthWiseContribDF = spark.sql("select gatherer_name,month, sum(Earning) as Max_Earning from EarningMonthWiseTable group by month,gatherer_name")

  val leastContriMonthWise = monthWiseContribDF
    .withColumn("max_qty", dense_rank().over(Window.partitionBy("month").orderBy("Max_Earning")))
    .filter(col("max_qty").equalTo(1))
    .drop(col("max_qty"))


  println("Least Contribution of Gatherer Monthly")
  leastContriMonthWise.show()

  val MaxContriMonthWise = monthWiseContribDF
    .withColumn("max_qty", dense_rank().over(Window.partitionBy("month").orderBy(desc("Max_Earning"))))
    .filter(col("max_qty").equalTo(1))
    .drop(col("max_qty"))

  println("Highest Contribution of Gatherer Monthly")
  MaxContriMonthWise.show()

  
}
