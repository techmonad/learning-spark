package com.techmonad.learn

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

object StructuredStreamingOps extends SparkSessionProvider {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    /**
     * Connect to streaming data source
     */
    val streamDF: DataFrame =
      spark
        .readStream
        .format("socket")
        .option("port", 9999)
        .option("host", "localhost")
        .load()

    /**
     * Apply transformation on the stream DataFrame
     */
    val words: DataFrame =
      streamDF
        .withColumn("words", split($"value", "\\s+"))
        .withColumn("word", explode($"words"))
        .select("word")
        .groupBy("word")
        .count()

    /**
     * write into down stream data source (Console is down steam data source for this example)
     */
    val query: StreamingQuery =
      words
        .writeStream
        .outputMode("complete")
        .format("console")
        .start()

    /**
     * wait for stream termination
     */
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
