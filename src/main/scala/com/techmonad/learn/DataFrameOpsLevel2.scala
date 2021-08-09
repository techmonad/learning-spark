package com.techmonad.learn


import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object DataFrameOpsLevel2 extends App with SparkSessionProvider {

  // top # elements by department
  //CSV => empid,ename,salary,deptId,mgtId

  val schema =
    StructType(
      Seq(
        StructField("empid", IntegerType, false),
        StructField("ename", StringType, false),
        StructField("salary", IntegerType, false),
        StructField("deptId", IntegerType, false),
        StructField("mgtId", IntegerType, true)
      )
    )

  val empDF =
    spark
      .read
      .schema(schema)
      .option("header", "true")
      .option("sep", ",")
      .csv("data/emp.csv")


  val partitionByDeptOrderBySal: WindowSpec =
    Window
      .partitionBy("deptId")
      .orderBy(desc("salary"))


  // TOP 5 salary in each department
  empDF
    .withColumn("rank", dense_rank() over partitionByDeptOrderBySal)
    .filter("rank <= 5")
    .select("deptId", "salary")
    .show(20)


  /** Expensive apprach
   *spark.udf.register("sort_list",(list:List[Int]) =>   list.sorted.take(3))
   * *
   * empDF
   * .groupBy("deptId")
   * .agg(collect_list($"salary").as("list"))
   * .selectExpr("deptId", "sort_list(list)")
   * .show()
   *
   */


}
