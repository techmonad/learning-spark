package com.techmonad.learn.jdbc

import com.techmonad.learn.SparkSessionProvider

/**
 * Postgres SQL queries:
 * *
 * postgres=# create database test_db;
 * CREATE DATABASE
 * postgres=# \c test_db;
 * You are now connected to database "test_db" as user "postgres".
 * test_db=# \d
 * Did not find any relations.
 * test_db=# create table employee(id int primary key, name varchar(50), salary int, dept_id int, manager_id int );
 * CREATE TABLE
 * test_db=# insert into employee values(1, 'bob', 50, 1, 5);
 * INSERT 0 1
 * test_db=# insert into employee values(2, 'Rob', 500, 1, 5);
 * INSERT 0 1
 * test_db=# insert into employee values(3, 'Ravi', 1000, 1, 4);
 * INSERT 0 1
 * test_db=# insert into employee values(4, 'Rajan', 5000, 1, 0);
 * INSERT 0 1
 * test_db=# insert into employee values(5, 'Ragu', 5000, 1, 0);
 * INSERT 0 1
 * test_db=# insert into employee values(6, 'Boby', 4000, 2, 3);
 * INSERT 0 1
 * test_db=# insert into employee values(7, 'Bably', 3000, 2, 3);
 * INSERT 0 1
 * test_db=# insert into employee values(8, 'Boby singh', 2000, 2, 4);
 * INSERT 0 1
 * test_db=# insert into employee values(9, 'BabaJI' , 5000, 2, 4);
 * INSERT 0 1
 * test_db=# insert into employee values(10, 'Bajarangi' , 6000, 2, 0);
 * INSERT 0 1
 * test_db=# insert into employee values(11, 'Bajigar' , 8000, 2, 10);
 * INSERT 0 1
 * ### MAX salary by department
 * test_db=# select dept_id, max(salary) from employee group by dept_id order by dept_id ;
 * 1 | 5000
 * 2 | 8000
 * *
 * #########Employee name with manager name
 * test_db=# select e.name,m.name from employee e, employee m where e.manager_id=m.id;
 * bob        | Ragu
 * Rob        | Ragu
 * Ravi       | Rajan
 * Boby       | Ravi
 * Bably      | Ravi
 * Boby singh | Rajan
 * BabaJI     | Rajan
 * Bajigar    | Bajarangi
 * ########## find the employees whose salary are greater than manager
 * test_db=# select e.name, e.salary ,m.name as manager, m.salary manager_salary  from employee e, employee m where e.manager_id=m.id and e.salary > m.salary;
 * name   | salary |  manager  | manager_salary
 * ---------+--------+-----------+----------------
 * Boby    |   4000 | Ravi      |           1000
 * Bably   |   3000 | Ravi      |           1000
 * Bajigar |   8000 | Bajarangi |           6000
 * (3 rows)
 * ###########top two salary by department
 * test_db=# select e.dept_id, e.name, e.salary from  (select  dept_id, name, salary, row_number() over (partition by dept_id order by salary desc) as rank from employee) e  where rank < 3;
 * dept_id |   name    | salary
 * ---------+-----------+--------
 * 1 | Rajan     |   5000
 * 1 | Ragu      |   5000
 * 2 | Bajigar   |   8000
 * 2 | Bajarangi |   6000
 * (4 rows)
 *
 *
 */

object JDBCDataFrameOps extends SparkSessionProvider {
  /**
   * SQL queries
   *
   */

  def main(args: Array[String]): Unit = {
    val df =
      spark
        .read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://localhost/test_db?user=postgres&password=postgres")
        .option("dbtable", "employee")
        .load()
    df.createOrReplaceTempView("emp")

    /* ###########All ROWS #############################*/
    spark.sql("select * from emp")
    /*
          +---+----------+------+-------+----------+
          | id|      name|salary|dept_id|manager_id|
          +---+----------+------+-------+----------+
          |  1|       bob|    50|      1|         5|
          |  2|       Rob|   500|      1|         5|
          |  3|      Ravi|  1000|      1|         4|
          |  4|     Rajan|  5000|      1|         0|
          |  5|      Ragu|  5000|      1|         0|
          |  6|      Boby|  4000|      2|         3|
          |  7|     Bably|  3000|      2|         3|
          |  8|Boby singh|  2000|      2|         4|
          |  9|    BabaJI|  5000|      2|         4|
          | 10| Bajarangi|  6000|      2|         0|
          | 11|   Bajigar|  8000|      2|        10|
          +---+----------+------+-------+----------+
     */


    /* ################## MAX salary by department ############################## */
    spark.sql("select dept_id, max(salary) max_salary from emp group by dept_id order by dept_id").show()
    /*
        +-------+----------+
        |dept_id|max_salary|
        +-------+----------+
        |      1|      5000|
        |      2|      8000|
        +-------+----------+
     */

    /*#########################Employee name with manager name ######################### */
    spark.sql("select e.name, m.name as manager_name from emp e, emp m where e.manager_id=m.id").show()
    /*
      +----------+------------+
      |      name|manager_name|
      +----------+------------+
      |      Boby|        Ravi|
      |     Bably|        Ravi|
      |       bob|        Ragu|
      |       Rob|        Ragu|
      |      Ravi|       Rajan|
      |Boby singh|       Rajan|
      |    BabaJI|       Rajan|
      |   Bajigar|   Bajarangi|
      +----------+------------+
*/

    /*########################### find the employees whose salary are greater than manager ######################*/
    spark.sql("select e.name, e.salary, m.name m_name, m.salary m_salary from emp e, emp m where e.manager_id=m.id and e.salary > m.salary").show()
    /*
         +-------+------+---------+--------+
         |   name|salary|   m_name|m_salary|
         +-------+------+---------+--------+
         |   Boby|  4000|     Ravi|    1000|
         |  Bably|  3000|     Ravi|    1000|
         |Bajigar|  8000|Bajarangi|    6000|
         +-------+------+---------+--------+
    */

    /* ################top two salaries by department #####################################################*/
    val sql = "select er.dept_id, er.salary from  (select dept_id, salary, rank() over (partition by dept_id order by salary desc) as rank from emp ) er  where rank <3 "
    spark.sql(sql).show()

    /*
     +-------+------+
        |dept_id|salary|
        +-------+------+
        |      1|  5000|
        |      1|  5000|
        |      2|  8000|
        |      2|  6000|
        +-------+------+
     */

  }

}
