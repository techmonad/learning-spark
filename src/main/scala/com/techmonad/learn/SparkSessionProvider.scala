package com.techmonad.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait SparkSessionProvider {

  val spark =
    SparkSession
      .builder()
      .appName("LearningSpark")
      .master("local[*]")
      .getOrCreate()

  val sc = spark.sparkContext


}

trait SparkContextProvider {

  implicit val sc = new SparkContext(
    new SparkConf()
      .setAppName("LearningSpark")
      .setMaster("local[*]")
  )


}
