package com.techmonad.learn.dataframe

import com.techmonad.learn.SparkSessionProvider
import org.apache.spark.sql.functions._

object DataFramesOpsLevel3 extends App with SparkSessionProvider {

  //PIVOTING(Swap the columns for rows) example
  //find the Monthly delays(max, avg) by destination for a origin
  // csv headers =>     date,delay, distance,origin,destination

  import spark.implicits._

  val df =
    spark
      .read
      .option("header", "true")
      .csv("data/departuredelays.csv")


  df
    .withColumn("month", expr("CAST(SUBSTRING(date, 0, 2) as Int)"))
    .withColumn("month",
      when($"month" === 1, "JAN")
        .when($"month" === 2, "FEB")
        .otherwise("Other_months")
    )
    .filter($"origin" === "SEA")
    .groupBy("destination")
    .pivot("month", Seq("JAN", "FEB"))
    .agg(max("delay").as("max_delay"), round(avg("delay"), 2).as("avg_delay"))
    .orderBy("destination")
   .show(false)


  // By SQL
  df.createOrReplaceTempView("departuredelays")

  spark
    .sql(
      """
        |select * from ( select destination, delay, CAST(SUBSTRING(date, 0, 2) as Int) as month  from departuredelays where origin='SEA')
        |PIVOT( CAST(avg(delay) as Decimal(4,2)) as avg_delay, max(delay) as max_delay for month in(1 JAN, 2 FEB) )
        |ORDER BY destination
        |""".stripMargin)
    //.show(false)


}
