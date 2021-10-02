package com.techmonad.learn


import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
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
      .orderBy(desc("salary"))


  // TOP 5 salary in each department
  empDF
    .withColumn("rank", dense_rank() over partitionByDeptOrderBySal)
    .filter("rank <= 5")
    .select("deptId", "salary")
    //.show(20)

// Salary in more readable format like upper(salary greater than 70), lower(salary less than 50) and medium(salary b/w 50 & 70) class

  empDF
    .withColumn("Class",
      when($"salary" <= 50 , "Lower Class" )
        .when($"salary" >50 && $"salary" <= 70, "Medium Class")
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
      | """.stripMargin).show()



}
