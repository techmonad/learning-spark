package com.techmonad.learn.hdfs

import com.techmonad.learn.SparkSessionProvider

object HdfsOps extends App with SparkSessionProvider {
  // Setup HDFS => https://phoenixnap.com/kb/install-hadoop-ubuntu

  import spark.implicits._


  // Write into HDFS
  val dataDF = (1 to 100000).toDF("numbers")

  dataDF
    .repartition(1)
    .write
    .format("csv")
  //  .save("hdfs://127.0.0.1:9000/spark/csv-data")


  //read from HDFS
  val df =
    spark
      .read
      .format("csv")
      .load("hdfs://127.0.0.1:9000/spark/csv")


  df.show()


}
