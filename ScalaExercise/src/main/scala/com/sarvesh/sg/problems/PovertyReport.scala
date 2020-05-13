package com.sarvesh.sg.problems

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, concat, lit, translate, regexp_replace, sum, max, round }

/**
 * End Goal:
 * 1.	Column State should be mapped with states name from
 * 2. Column Area_name should have State suffix e.g. â€œClay Countyâ€ will become â€œClay Country ALâ€
 * 3.	Select only odd values from column Urban_Influence_Code_2003
 * 4.	Select only even values from column Rural-urban_Continuum_Code_2013
 * 5. Find out estimated percent of people age >17 in poverty 2018.
 * 6. Final Column :
 *
 * Update1: We have corrected the state master dataset as it is a Slowly Changing Dimension
 * and In our case it is going to change rarely.
 */

object PovertyReport {
  def main(args: Array[String]): Unit = {

    if (args.length == 0 || args.length > 3) {
      println("	Please provide the filepath in argument only")
      System.exit(1)
    }

    val filePathInput = args(0)
    val filePathState = args(1)
    val outputFilePath = args(2)
    
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Wordount")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    import spark.implicits._
    val povertyDf = spark.read.format("com.crealytics.spark.excel")
      .option("dataAddress", "'Poverty Data 2018'!A5")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .option("addColorColumns", "false")
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .load(filePathInput)

    val povertyDf2 = povertyDf.select(col("Stabr"), concat(col("Area_name"), lit(" "), col("Stabr")).as("Area_name"),
      col("Urban_Influence_Code_2003") cast "Int",
      col("Rural-urban_Continuum_Code_2013") cast "Int",
      regexp_replace(col("POVALL_2018"), """\s*,\s*""", "").as("POVALL_2018") cast "Long",
      regexp_replace(col("POV017_2018"), """\s*,\s*""", "").as("POV017_2018") cast "Long",
      col("PCTPOV017_2018"))
      .filter(col("Rural-urban_Continuum_Code_2013") % 2 === 0 && col("Urban_Influence_Code_2003") % 2 === 1)
    
    val povertyDf3 = povertyDf2.withColumn("POV_elder_than17_2018", round(((col("POVALL_2018") - col("POV017_2018")) / col("POVALL_2018")) * 100, 2)).drop("POVALL_2018", "POV017_2018", "PCTPOV017_2018")

    val stateDF = spark.read.format("com.crealytics.spark.excel")
      .option("dataAddress", "'Sheet2'!A1")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "false")
      .option("addColorColumns", "false")
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .load(filePathState)

    val finalDF = povertyDf3.join(stateDF, povertyDf3("Stabr") === stateDF("Postal Abbreviation"), "Left")
      .drop("Stabr", "Postal Abbreviation")
      .select(col("Capital Name").as("state"), col("Area_name"), col("Urban_Influence_Code_2003"), col("Rural-urban_Continuum_Code_2013"), col("POV_elder_than17_2018"))
    finalDF.show()
    finalDF.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'PovertyResultset'!A1")
      .option("header", "true")
      .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("overwrite") // Optional, default: overwrite.
      .save(outputFilePath+"/PovertyReport.xlsx")
  }
}