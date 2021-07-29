package com.techmonad.learn

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

object StructuredStreamingOps extends SparkSessionProvider {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val streamDF: DataFrame =
      spark
        .readStream
        .format("socket")
        .option("port", 9999)
        .option("host", "localhost")
        .load()

    val words: DataFrame =
      streamDF
        .withColumn("words", split($"value", "\\s+"))
        .withColumn("word", explode($"words"))
        .select("word")
        .groupBy("word")
        .count()

    val query: StreamingQuery =
      words
        .writeStream
        .outputMode("complete")
        .format("console")
        .start()

    query.awaitTermination()

    /*    words
          .writeStream
          .foreachBatch { (df, batchNo) =>
            df.show()
          }
          .start()
          .awaitTermination()*/


  }

}
