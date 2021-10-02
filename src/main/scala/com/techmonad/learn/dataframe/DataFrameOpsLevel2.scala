package com.techmonad.learn.dataframe

import com.techmonad.learn.SparkSessionProvider
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{asc, dense_rank, desc, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataFrameOpsLevel2 extends App with SparkSessionProvider {

  import spark.implicits._
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
      .orderBy(asc("deptId"), desc("salary"))

  // TOP 5 salary in each department
  empDF
    .withColumn("rank", dense_rank() over partitionByDeptOrderBySal)
    .filter("rank <= 3")
    .select("deptId", "salary", "rank")
  //  .show()

  // Salary in more readable format like upper(salary greater than 70), lower(salary less than 50) and medium(salary b/w 50 & 70) class

  empDF
    .withColumn("Class",
      when($"salary" <= 50, "Lower Class")
        .when($"salary" > 50 && $"salary" <= 70, "Medium Class")
        .otherwise("Upper Class")
    )
  // .show()
  // OR by SQL

  empDF.createOrReplaceTempView("emp")

  spark.sql(
    """select ename,salary,
      | CASE
      |    when salary <= 50  then "Lower Class"
      |    when salary > 50 AND salary <= 70 then "Medium Class"
      |    else "Upper Class"
      |    END as Class
      |    from emp;
      | """.stripMargin)
  //.show()


}
