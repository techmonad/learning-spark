package com.techmonad.learn.dataset

import com.techmonad.learn.SparkSessionProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders}


object DataSetOps extends SparkSessionProvider {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    // read text file as a dataset
    val ds: Dataset[String] =
      spark
        .read
        .textFile("data/words.txt")

    println("############# Word Count ###################")
    val wordCounts: Dataset[(String, Long)] =
      ds
        .flatMap { line =>
          line
            .split("\\s+")
            .map(_.trim)
        }
        .filter { word => word.length > 0 }
        .groupByKey { word => word }
        .count()

    wordCounts.show()

    // read csv file as a dataset
    val users: Dataset[User] =
      spark
        .read
        .schema(Encoders.product[User].schema)
        .option("delimiter", ",")
        .option("header", "true")
        .csv("data/users.csv")
        .as[User]
    users.show()

    val userDetails: Dataset[Detail] =
      spark
        .read
        .schema(Encoders.product[Detail].schema)
        .option("header", "true")
        .option("delimiter", ",")
        .csv("data/user-details.csv")
        .as[Detail]

    userDetails.show()

    /**
     * Joins - inner, left, right, ful
     **/

    // inner join(by default is inner join)
    // val innerJoin = users.join(userDetails, "id")
    // more explicit
    println("#############Inner Join###################")
    val innerJoin: Dataset[UserDetails] = users.join(userDetails, Seq("id"), "inner").as[UserDetails]
    innerJoin.show()

    println("#############Left Join###################")
    val leftJoin: Dataset[UserDetails] = users.join(userDetails, Seq("id"), "left").as[UserDetails]
    leftJoin.show()

    println("#############Right Join###################")
    val rightJoin: Dataset[UserDetails] = users.join(userDetails, Seq("id"), "right").as[UserDetails]
    rightJoin.show()


    println("#############full Join###################")
    val fullJoin: Dataset[UserDetails] = users.join(userDetails, Seq("id"), "full").as[UserDetails]
    fullJoin.show()

    println("#############JoinWith ###################")
    val usingJoinWith: Dataset[(User, Detail)] = users.joinWith(userDetails, users("id") === userDetails("id"))
    usingJoinWith.show()

    // Agg
    // Get max, avg, min  salary
    println("############# Max  salary###################")
    innerJoin
      .groupBy($"name")
      .agg(max("salary").as("max salary"), avg("salary").as("Average salary"), min("salary").as("min salary"))
      .show()

    spark.stop()
  }
}

case class User(id: Int, name: String, email: String)

case class Detail(id: Int, employer: String, location: String, salary: Int)

case class UserDetails(id: Option[Int], name: Option[String], email: Option[String], employer: Option[String], location: Option[String], salary: Option[Int])