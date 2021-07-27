package com.techmonad.learn

import org.apache.spark.rdd.RDD

object RDDOps extends SparkSessionProvider {

  def main(args: Array[String]): Unit = {

    // text file reading
    val rdd: RDD[String] =
      sc
        .textFile("data/words.txt")

    println("############ Word count ##############################")
    val wordCounts: RDD[(String, Int)] =
      rdd
        .flatMap { line =>
          line
            .split("\\s+")
            .filter { word => word.length > 0 }
        }
        .map { word => (word, 1) }
        .reduceByKey { case (count1, count2) => count1 + count2 }

    wordCounts.collect.foreach(println)


    val users: RDD[User] =
      sc
        .textFile("data/users.csv")
        .mapPartitionsWithIndex { case (index, itr) => if (index == 0) itr.drop(1) else itr }
        .map { line =>
          val arr =
            line
              .split(",")
              .map(_.trim)
          User(arr(0).toInt, arr(1), arr(2))
        }

    users.collect.foreach(println)

    val details: RDD[Detail] =
      sc
        .textFile("data/user-details.csv")
        .mapPartitionsWithIndex { case (index, itr) => if (index == 0) itr.drop(1) else itr }
        .map { line =>
          val arr =
            line
              .split(",")
              .map(_.trim)
          Detail(arr(0).toInt, arr(1), arr(2), arr(3).toInt)
        }

    details.collect.foreach(println)


    val userWithId: RDD[(Int, User)] =users.map { user => (user.id, user) }
    val detailWithId: RDD[(Int, Detail)] = details.map{ detail => (detail.id, detail) }
    // Joins in RDD
    val userDetails: RDD[(Int, (User, Detail))] = userWithId.join(detailWithId)

     userDetails.collect.foreach(println)

    val userDetailsLeft: RDD[(Int, (User, Option[Detail]))] = userWithId.leftOuterJoin(detailWithId)
    userDetailsLeft.collect.foreach(println)

    val userDetailsRight: RDD[(Int, (Option[User], Detail))] = userWithId.rightOuterJoin(detailWithId)
    userDetailsRight.collect.foreach(println)

    spark.stop()
  }


}
