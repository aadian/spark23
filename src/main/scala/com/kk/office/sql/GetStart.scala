package com.kk.office.sql

import javax.annotation.Nullable

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StringType, StructField}

import org.apache.spark.sql.types._

/**
  * Created by kevin on 25/5/18.
  */
object GetStart {


  case class people(name: String, age: Long)


  def main(args: Array[String]) {


    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("SQL Get Start")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    //GetStart(spark)


    //CreateDS(spark)

    //InferSchemaWithReflect(spark)

    //SpecificSchema(spark)


    //val (userDF: DataFrame, peopleDF: DataFrame, sqlDF: DataFrame) = GenericLoadAndSave(spark)

    SchemaMerge(spark)


  }

  def SchemaMerge(spark: SparkSession): Unit = {

    import spark.implicits._

    val squareDF = spark.sparkContext.makeRDD((1 to 5).map(i => (i, i * i))).toDF("value", "square")
    squareDF.write.parquet("resources/data/test_table/key=1")

    val cubeDF = spark.sparkContext.makeRDD((1 to 5).map(i => (i, i * i * i))).toDF("value", "cube")
    cubeDF.write.parquet("resources/data/test_table/key=2")

    val mergeDF = spark.read.option("mergeSchema", true).parquet("resources/data/test_table")

    mergeDF.printSchema()
    mergeDF.show()
  }

  def GenericLoadAndSave(spark: SparkSession): (sql.DataFrame, sql.DataFrame, sql.DataFrame) = {
    /**
      * In the simplest form, the default data source (parquet unless otherwise configured
      * by spark.sql.sources.default) will be used for all operations
      */

    val userDF = spark.read.load("src/spark-master/examples/src/main/resources/users.parquet")
    userDF.select("name", "favorite_color").write.save("resources/namesAndFavColors.parquet")


    /**
      * Data sources are specified by their fully qualified name (i.e., org.apache.spark.sql.parquet),
      * but for built-in sources you can also use their short names (json, parquet, jdbc, orc, libsvm, csv, text).
      * DataFrames loaded from any data source type can be converted into other types using this syntax.
      */
    val peopleDF = spark.read.format("json").load("src/spark-master/examples/src/main/resources/people.json")
    peopleDF.select("name", "age").write.format("parquet").save("resources/namesAndAges.parquet")


    /**
      * To load a CSV file you can use:
      */
    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", true)
      .option("header", true)
      .load("src/spark-master/examples/src/main/resources/people.csv").show()


    //For file-based data source, it is also possible to bucket and sort or partition the output.
    //Bucketing and sorting are applicable only to persistent tables:
    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")

    // partitioning can be used with both save and saveAsTable when using the Dataset APIs
    userDF.write.partitionBy("favorite_color").format("parquet").save("resources/namesPartByColor.parquet")

    //usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

    //It is possible to use both partitioning and bucketing for a single table:
    userDF.write.bucketBy(42, "name").partitionBy("favorite_color").format("parquet").saveAsTable("users_partitioned_bucketed")

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed")


    /**
      * Instead of using read API to load a file into DataFrame and query it, you can also query that file directly with SQL.
      */
    val sqlDF = spark.sql("SELECT * from parquet.`src/spark-master/examples/src/main/resources/users.parquet`")

    //For file-based data source, e.g. text, parquet, json, etc. you can specify a custom table path via the path option
    sqlDF.write.option("path", "resources/users").saveAsTable("users")

    sqlDF.show()
    (userDF, peopleDF, sqlDF)
  }

  def SpecificSchema(spark: SparkSession): Unit = {
    import spark.implicits._


    val schemaString = "name age"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val peopleRDD = spark.sparkContext.textFile("src/spark-master/examples/src/main/resources/people.txt")


    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    val peopleDF = spark.createDataFrame(rowRDD, schema)

    peopleDF.createOrReplaceTempView("people")

    val results = spark.sql("select * from people")
    results.map(attributes => "Name:" + attributes(0)).show()
  }


  def InferSchemaWithReflect(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleDF = spark.sparkContext.textFile("src/spark-master/examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => people(attributes(0), attributes(1).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")

    val sqlDF = spark.sql("select * from people")

    sqlDF.map(teenager => "Name: " + teenager(0)).show()

    sqlDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    sqlDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
    // Array(Map("name" -> "Justin", "age" -> 19))
  }

  def CreateDS(spark: SparkSession): Unit = {
    import spark.implicits._

    // Encoders are created for case classes
    val ds = Seq(people("Andy", 32)).toDS
    ds.show()

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ * 2).collect().foreach(println)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val peopleDF = spark.read.json("src/spark-master/examples/src/main/resources/people.json")
    val peopleDS = peopleDF.as[people]
    peopleDS.show()
  }

  def GetStart(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.read.json("src/spark-master/examples/src/main/resources/people.json")
    df.printSchema()
    df.filter($"age" > 19).show()
    df.select($"name", $"age" + 1).show()


    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select name, age from people where age > 19")
    sqlDF.show()

    df.createGlobalTempView("people")
    val sqlGlobalDF = spark.sql("select name, age+1 from global_temp.people")
    sqlGlobalDF.show()
  }
}
