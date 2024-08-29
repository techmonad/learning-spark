package com.techmonad.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait SparkSessionProvider {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("LearningSpark")
      .master("local[*]")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext


}

trait SparkContextProvider {

  implicit val sc: SparkContext = new SparkContext(
    new SparkConf()
      .setAppName("LearningSpark")
      .setMaster("local[*]")
  )


}
