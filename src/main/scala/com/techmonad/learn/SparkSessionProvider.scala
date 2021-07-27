package com.techmonad.learn

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  val spark =
    SparkSession
      .builder()
      .appName("LearningSpark")
      .master("local[*]")
      .getOrCreate()

  val sc = spark.sparkContext


}
