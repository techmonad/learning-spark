package com.techmonad.learn

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataFrameOps extends SparkSessionProvider {

  def main(args: Array[String]): Unit = {

    val df: DataFrame =
      spark
        .read
        .text("data/words.txt")
    println("###############Word Count################")
    val wordCounts =
      df
        .withColumn("words", split(col("value"), "\\s+"))
        .withColumn("word", explode(col("words")))
        .select("word")
        .groupBy("word")
        .agg(count("word").as("count"))
    wordCounts.show()

    // Joins
    val users =
      spark
        .read
        .option("delimiter", ",")
        .option("header", "true")
        .csv("data/users.csv")
    users.show()

    val userDetails =
      spark
        .read
        .option("header", "true")
        .option("delimiter", ",")
        .csv("data/user-details.csv")

    userDetails.show()

    println("#############Inner Join###################")
    val innerJoin: DataFrame = users.join(userDetails, Seq("id"), "inner")
    innerJoin.show()

    println("#############Left Join###################")
    val leftJoin: DataFrame = users.join(userDetails, Seq("id"), "left")
    leftJoin.show()

    println("#############Right Join###################")
    val rightJoin: DataFrame = users.join(userDetails, Seq("id"), "right")
    rightJoin.show()


    println("#############full Join###################")
    val fullJoin: DataFrame = users.join(userDetails, Seq("id"), "full")
    fullJoin.show()

    spark.stop()
  }

}
