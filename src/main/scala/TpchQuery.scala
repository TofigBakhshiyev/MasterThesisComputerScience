package main.scala

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.io.PrintWriter

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   * Implemented in children classes and holds the actual query
   */
  def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {
  /* def createEmptyCSVFile(spark: SparkSession, fileFormat: String): Unit = {
    // Create an empty DataFrame with no schema
    // Define an empty schema with a single dummy column
    val schema = StructType(Seq(StructField("dummyColumn", StringType, nullable = true)))

    // Create an empty DataFrame with the specified schema
    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    // Write the empty DataFrame to a CSV file
    emptyDF
      .coalesce(1)  // optional: reduce the number of output files to one
      .write
      .csv("file:/home/spark/Desktop/tpch-spark/queryEnergyResults/"+ fileFormat +".csv")

  } */

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else {
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
    }
  }

  def executeQueries(spark: SparkSession, schemaProvider: TpchSchemaProvider, queryNum: Option[Int], queryOutputDir: String): ListBuffer[(String, Float)] = {
    var queryFrom = 1;
    var queryTo = 22;
    queryNum match {
      case Some(n) => {
        queryFrom = n
        queryTo = n
      }
      case None => {}
    }

    val executionTimes = new ListBuffer[(String, Float)]
    for (queryNo <- queryFrom to queryTo) {
      val startTime = System.nanoTime()
      val query_name = f"main.scala.Q${queryNo}%02d"

      val log = LogManager.getRootLogger
      try {
        val query = Class.forName(query_name).newInstance.asInstanceOf[TpchQuery]
        val queryOutput = query.execute(spark, schemaProvider)
        outputDF(queryOutput, queryOutputDir, query.getName())

        val endTime = System.nanoTime()
        val elapsed = (endTime - startTime) / 1000000000.0f // to seconds
        executionTimes += new Tuple2(query.getName(), elapsed)
      }
      catch {
        case e: Exception => log.warn(f"Failed to execute query ${query_name}: ${e}")      }
    }

    return executionTimes
  }

  def main(args: Array[String]): Unit = {
    // parse command line arguments: expecting _at most_ 1 argument denoting which query to run
    // if no query is given, all queries 1..22 are run.
    if (args.length > 1)
      println("Expected at most 1 argument: query to run. No arguments = run all 22 queries.")
    val queryNum = if (args.length == 3) {
      try {
        Some(Integer.parseInt(args(0).trim))
      } catch {
        case e: Exception => None
      }
    } else
      None

    val fileFormat = args(1).trim
    val numPartitions = args(2).trim

    // get paths from env variables else use default
    val cwd = System.getProperty("user.dir")
    var inputDataDir = sys.env.getOrElse("TPCH_INPUT_DATA_DIR", "file://" + cwd + "/dbgen")
    if (fileFormat == "csv") {
      inputDataDir = sys.env.getOrElse("TPCH_INPUT_DATA_DIR", "file://" + cwd + "/csv")
      println(inputDataDir)
    }
    else if (fileFormat == "parquet") {
      inputDataDir = sys.env.getOrElse("TPCH_INPUT_DATA_DIR", "file://" + cwd + "/parquet")
      println(inputDataDir)
    }
    else if (fileFormat == "avro") {
      inputDataDir = sys.env.getOrElse("TPCH_INPUT_DATA_DIR", "file://" + cwd + "/avro")
      println(inputDataDir)
    }
    val queryOutputDir = sys.env.getOrElse("TPCH_QUERY_OUTPUT_DIR", inputDataDir + "/output")
    val executionTimesPath = sys.env.getOrElse("TPCH_EXECUTION_TIMES", cwd + "/tpch_execution_times.csv")

    val spark = SparkSession
      .builder
      .appName("TPC-H v3.5.0 Spark")
      //.config("spark.master", "local")
      .config("spark.default.parallelism", numPartitions)
      .getOrCreate()

    val schemaProvider = new TpchSchemaProvider(spark, inputDataDir, fileFormat)

    // execute queries
    val executionTimes = executeQueries(spark, schemaProvider, queryNum, queryOutputDir)
    spark.close()

    // write execution times to file
    if (executionTimes.length > 0) {
      val outfile = new File(executionTimesPath)
      val bw = new BufferedWriter(new FileWriter(outfile, true))

      bw.write("Query,NumPartitions,FileFormat,Time(seconds)\n")
      executionTimes.foreach {
        case (key, value) => 
        bw.write(s"$key,$numPartitions,$fileFormat,${value}\n")
      }
      bw.close()

      println(f"Execution times written in ${outfile}.")
    }

    println("Execution complete.")
  }
}
