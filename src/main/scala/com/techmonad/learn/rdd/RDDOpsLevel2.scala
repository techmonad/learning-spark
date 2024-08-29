package com.techmonad.learn.rdd

import com.techmonad.learn.SparkContextProvider
import org.apache.spark.rdd.RDD

object RDDOpsLevel2 extends SparkContextProvider {

  def main(args: Array[String]): Unit = {
    // operations on pair RDD

    // find the average of score of each players
    val seq = Seq(("bob", 12), ("rob", 24), ("raj", 35), ("bob", 88), ("rob", 76), ("raj", 65))

    // combine by key
    //createCombiner: V => C,
    // mergeValue: (C, V) => C,
    // mergeCombiners: (C, C) => C,
    val rdd: RDD[(String, Int)] =
    sc
      .parallelize(seq)
    // a => b
    // b, a => b
    // b,b => b
    val result1: RDD[(String, (Int, Int))] =
    rdd
      .combineByKey(
        score => (1, score),
        (score: (Int, Int), scoreWithCount: Int) => (score._1 + 1, score._2 + scoreWithCount),
        (s1: (Int, Int), s2: (Int, Int)) => (s1._1 + s2._1, s1._2 + s2._2)
      )


    // by aggregateByKey
    val result2: RDD[(String, (Int, Double))] =
      rdd
        .aggregateByKey((0, 0.0))((acc, v) => (acc._1 + 1, acc._2 + v), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))



    // reduce by Key
    val result3: RDD[(String, (Int, Int))] =
      rdd
        .mapValues(score => (1, score))
        .reduceByKey { (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2) }


    result3
      .map { case (k, (count, score)) => (k, score * 1.0 / count) }
      .collect
      .foreach(println)


  }

}
