package com.urbancomputing

import com.urbancomputing.app.DataProcessApp._
import com.urbancomputing.app.DataAnalysisApp._
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Envelope

object Main {
  private val conf = new SparkConf().setAppName("ST-KNN")
  private val spark = new SparkContext(conf)

  def taxiSpatialTemporalOp(inputHdfsPath: String, outputHdfsPath: String): Unit = {
    val rawData = spark.textFile(inputHdfsPath)
    val (oRdd, dRdd) = taxiOD(rawData)
    oRdd.saveAsTextFile(outputHdfsPath + "_O")
    dRdd.saveAsTextFile(outputHdfsPath + "_D")
  }

  def lorrySpatialOp(inputHdfsPath: String, outputHdfsPath: String): Unit = {
    val rawData = spark.textFile(inputHdfsPath)
    val cleanData = lorrySpatial(rawData)
    cleanData.saveAsTextFile(outputHdfsPath)
  }

  def lorrySpatialTemporalOp(inputHdfsPath: String, outputHdfsPath: String): Unit = {
    val rawData = spark.textFile(inputHdfsPath)
    val cleanData = lorrySpatialTemporal(rawData)
    cleanData.saveAsTextFile(outputHdfsPath)
  }

  def geomAnalysisOp(hdfsPath: String, geomType: String): Unit = {
    val data = spark.textFile(hdfsPath)
    val (totalNum, totalPointNum, avgPointNum) = {
      geomType.toLowerCase match {
        case "point" => point(data)
        case "linestring" => lineString(data)
        case "polygon" => polygon(data)
        case _ => throw new IllegalArgumentException(s"[$geomType] is not supported")
      }
    }
    println(
      s"""
         |+++++++
         |line string analysis: [hdfs path = $hdfsPath, geom type = $geomType]
         |row count:            [$totalNum]
         |total point number:   [$totalPointNum]
         |avg point number:     [$avgPointNum]
         |+++++++
         |""".stripMargin)
  }

  def extractOp(inputHdfsPath: String, mbrParma: String): Unit = {
    val rawData = spark.textFile(inputHdfsPath)
    val coords = mbrParma.split("_").map(_.toDouble)
    val mbr = new Envelope(coords(0), coords(1), coords(2), coords(3))
    val extractedData = extractByMBR(rawData, mbr)
    // minLng_maxLng_minLat_maxLat
    val outputHdfsPath = inputHdfsPath + "_" + mbrParma
    extractedData.saveAsTextFile(outputHdfsPath)
    println(
      s"""
         |+++++++
         |extractor:  [inputHdfsPath=$inputHdfsPath, parma=$mbrParma, outputHdfsPath=$outputHdfsPath]
         |data count: [${rawData.count()} to ${extractedData.count()}]
         |+++++++
         |""".stripMargin)
  }

  def didiToOldJustOp(inputHdfsPath: String, outputHdfsPath: String): Unit = {
    val rawData = spark.textFile(inputHdfsPath)
    val cleanData = didiToOldJust(rawData)
    cleanData.saveAsTextFile(outputHdfsPath)
  }

  def didiToNewJustOp(inputHdfsPath: String, outputHdfsPath: String): Unit = {
    val rawData = spark.textFile(inputHdfsPath)
    val cleanData = didiToNewJust(rawData)
    cleanData.saveAsTextFile(outputHdfsPath)
  }

  def main(args: Array[String]): Unit = {
    val op = args(0).toLowerCase
    op match {
      case "taxi-st" =>
        assert(args.length == 3)
        val (inputHdfsPath, outputHdfsPath) = (args(1), args(2))
        taxiSpatialTemporalOp(inputHdfsPath, outputHdfsPath)

      case "lorry-s" =>
        assert(args.length == 3)
        val (inputHdfsPath, outputHdfsPath) = (args(1), args(2))
        lorrySpatialOp(inputHdfsPath, outputHdfsPath)

      case "lorry-st" =>
        assert(args.length == 3)
        val (inputHdfsPath, outputHdfsPath) = (args(1), args(2))
        lorrySpatialTemporalOp(inputHdfsPath, outputHdfsPath)

      case "geom-analysis" =>
        assert(args.length == 3)
        val (hdfsPath, geomType) = (args(1), args(2))
        geomAnalysisOp(hdfsPath, geomType)

      case "extract" =>
        assert(args.length == 3)
        val (inputHdfsPath, mbrParma) = (args(1), args(2))
        extractOp(inputHdfsPath, mbrParma)

      case "didi-to-old-just" =>
        assert(args.length == 3)
        val (inputHdfsPath, outputHdfsPath) = (args(1), args(2))
        didiToOldJustOp(inputHdfsPath, outputHdfsPath)

      case "didi-to-new-just" =>
        assert(args.length == 3)
        val (inputHdfsPath, outputHdfsPath) = (args(1), args(2))
        didiToNewJustOp(inputHdfsPath, outputHdfsPath)

      case _ => throw new IllegalArgumentException(s"[$op] is not supported")
    }
    spark.stop()
  }
}
