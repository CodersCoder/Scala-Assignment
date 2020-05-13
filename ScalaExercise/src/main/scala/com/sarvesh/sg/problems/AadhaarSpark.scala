package com.sarvesh.sg.problems

import org.apache.spark.sql.SparkSession
import scala.util.Try
import scala.util.Success
import org.apache.spark.rdd.RDD
import java.io.FileNotFoundException
import scala.util.Failure

/**
 * Assumptions: 
 * 1. SA,AUA and res_state_name present at 3,2 and 128 index in each line. 
 * 2. If there are lesser records in a line than 128, then that row will not be included in the
 * 		final answer.
 * 3. Also filtered all the rows in which AUA is not a numeric value to compare that with 650000.
 */

object AadhaarSpark {

  def main(args: Array[String]): Unit = {

    if (args.length == 0 || args.length > 2) {
      println("	Please provide the file path in argument only")
      System.exit(1)
    }

    val filePath = args(0)
    val outputPath = args(1)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Wordount")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    import spark.implicits._
    val startTIme = System.currentTimeMillis()
    
    def createRdd(fileName: String): RDD[Option[(String, String, String)]] = {
      spark.sparkContext.textFile(filePath).map(line => {
        val rowArray = line.split(",",-1)
        if (rowArray.size > 128) Some(rowArray(3), rowArray(2), rowArray(128)) else None
      })
    }

    val inputData = createRdd(filePath).filter(_ != None).map(_.get)

    val outputData = inputData.filter(inputTuple =>
      (isAllDigits(inputTuple._1) && isAllDigits(inputTuple._2) && inputTuple._2.toInt > 650000 && inputTuple._3.toLowerCase() != "delhi"))
      .map(_._2)
    outputData.persist()
    println(s"Records Processed: ${outputData.count()}")
    outputData.saveAsTextFile(outputPath+"/AadharSparkOutput")
    val endTime = (System.currentTimeMillis() - startTIme)
    println(s"Time taken : ${endTime} ms")
  }
}