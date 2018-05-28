package com.kk.ln.office.sql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by leiying on 2018/5/25.
  */
object InitLn {

  case class Person(name: String, age: Long)

  val personJsonPath = "E:\\spark\\spark23\\src\\spark-master\\examples\\src\\main\\resources\\people.json"

  val personJsonText = "E:\\spark\\spark23\\src\\spark-master\\examples\\src\\main\\resources\\people.txt"

  val employeesJsonPath = "E:\\spark\\spark23\\src\\spark-master\\examples\\src\\main\\resources\\employees.json"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("init start for spark sql").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //val df: DataFrame = UptypedDsOper(spark)

    //runningSQL(spark, df)

    //CreateDS(spark)

    //InferSchema(spark)

    //SpecificSchema(spark)

    //UntypedUserDefAgg(spark)

  }

  private def UntypedUserDefAgg(spark: SparkSession) = {
    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json(employeesJsonPath)
    df.createOrReplaceTempView("employees")
    df.show()

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
  }

  private def SpecificSchema(spark: SparkSession) = {
    import spark.implicits._

    val peopleRDD = spark.sparkContext.textFile(personJsonText)
    val schemaString = "name age"

    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    val peopleDF = spark.createDataFrame(rowRDD, schema)

    peopleDF.createOrReplaceTempView("people")

    val results = spark.sql("SELECT name FROM people")

    results.map(attributes => "Name:" + attributes(0)).show()
  }

  private def InferSchema(spark: SparkSession) = {

    import spark.implicits._

    val peopleDF = spark.sparkContext
      .textFile(personJsonText)
      .map(_.split(" "))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")

    val teenagersDF = spark.sql("select name, age FROM people WHERE age BETWEEN 13 and 19")

    teenagersDF.map(teenager => "Name:" + teenager(0)).show()

    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    val tenCol = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    tenCol.foreach(println)
  }

  private def CreateDS(spark: SparkSession) = {
    import spark.implicits._

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).show()

    val peopleDS = spark.read.json(personJsonPath).as[Person]

    peopleDS.show()
  }

  private def UptypedDsOper(spark: SparkSession) = {
    val df = spark.read.json(personJsonPath)
    df.show()

    import spark.implicits._

    df.printSchema()

    df.select("name").show()

    df.select($"name", $"age" + 1).show()

    df.filter($"age" >= 20).show()

    df.groupBy($"age").count().show()
    df
  }

  private def runningSQL(spark: SparkSession, df: DataFrame) = {
    //Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates.
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    //Global temporary view is tied to a system preserved database global_temp,
    // and we must use the qualified name to refer it,
    df.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()
  }
}
