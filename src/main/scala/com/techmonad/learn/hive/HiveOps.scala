package com.techmonad.learn.hive

import org.apache.spark.sql.SparkSession

object HiveOps extends App {

  // hive setup => https://phoenixnap.com/kb/install-hive-on-ubuntu
  // start meta service => $ hive --service metastore

  val spark =
    SparkSession
      .builder()
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://localhost:9083") // configure meta store
      .enableHiveSupport()
      .getOrCreate()

  import spark.implicits._

  // show tables from default database (you can change by sql("use database_name"))
  spark.sql("show tables").show()

  // create a table into hive
  val ddl = "CREATE TABLE Employee (id Int, name STRING, email STRING, location String) PARTITIONED BY (dob date);"
  // spark.sql(ddl).show(false)

  spark.sql("desc Employee").show(false)


  val df =
    Seq(
      (1, "bob", "bob@gmail.com", "SFO", "11/24/1995"),
      (2, "roy", "roy@gmail.com", "NY", "01/23/1991"),
      (3, "rob", "rob@gmail.com", "AT", "07/20/1993")
    )
      .toDF("id", "name", "email", "location", "dob")

  // write into hive table
  df
    .write
    .mode("overwrite")
    .saveAsTable("Employee")

  // Read from hive table

  // Using dataframe
  spark
    .read
    .table("Employee")
    .show()

  //Using SQL

  spark.sql("select * from employee").show()

  spark.stop()


}
