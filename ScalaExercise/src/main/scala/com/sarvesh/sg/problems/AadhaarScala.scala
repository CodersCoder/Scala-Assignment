package com.sarvesh.sg.problems

import scala.io.Source
import scala.util.Try
import scala.util.Success
import java.io.PrintWriter
import java.io.File

/**
 * Assumptions: 
 * 1. SA,AUA and res_state_name present at 3,2 and 128 index in each line. 
 * 2. If there are lesser records in a line than 128, then that row will not be included in the
 * 		final answer.
 * 3. Also filtered all the rows in which AUA is not a numeric value to compare that with 650000.
 */

object AadhaarScala {
  def main(args: Array[String]): Unit = {

    if (args.length == 0 || args.length > 2) {
      println("	Please provide the file path in argument only")
      System.exit(1)
    }
    val InputFilePath = args(0)
    val outputFilePath = args(1)
    val startTIme = System.currentTimeMillis()

    val writer = new PrintWriter(new File(outputFilePath + "/AadhaarScalaOutput.csv"))
    
    val inputData = Source.fromFile(InputFilePath).getLines.map(line => {
      val rowArray = line.split(",", -1)
      Try(rowArray(3), rowArray(2), rowArray(128))
    })
    val successData = inputData.collect {
      case Success(row) => row
    }

    val outputData = successData.filter(inputTuple =>
      (isAllDigits(inputTuple._1) && isAllDigits(inputTuple._2) && inputTuple._2.toInt > 650000 && inputTuple._3.toLowerCase() != "delhi"))
      .map(_._2)

    outputData.foreach(line => {
      writer.write(line)
      writer.write("\n")
    })
    writer.close()
    val endTime = (System.currentTimeMillis() - startTIme)
    println(s"Time taken : ${endTime} ms")
  }
}