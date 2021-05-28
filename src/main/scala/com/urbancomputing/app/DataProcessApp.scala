package com.urbancomputing.app

import com.urbancomputing.util.WKTUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Envelope

import java.sql.Date
import java.text.SimpleDateFormat

object DataProcessApp {
  /**
   * to `2013-01-01 00:00:00,POINT (X1 Y1)`
   */
  @deprecated
  def taxi(data: RDD[String]): RDD[String] =
    data
      .filter(_.nonEmpty)
      .map(line => line.split(","))
      .filter(_.length == 14)
      .flatMap(arr => {
        val (pickUpTime, pickUpLoc) = (arr(5), "POINT (" + arr(10) + " " + arr(11) + ")")
        val (dropOffTime, dropOffLoc) = (arr(6), "POINT (" + arr(12) + " " + arr(13) + ")")
        Seq((pickUpTime, pickUpLoc), (dropOffTime, dropOffLoc))
      })
      .filter(_._2 != "POINT (0 0)")
      .map(t => t._1 + ", " + t._2)

  /**
   * to OD RDD, where 0 is ORIGIN and 1 is DESTINATION
   */
  def taxiOD(rdd: RDD[String]): (RDD[String], RDD[String]) = {
    val rawData = rdd.filter(_.nonEmpty)
      .map(line => line.split(",")).filter(_.length == 14).zipWithIndex()
    rawData.persist(StorageLevel.MEMORY_AND_DISK)

    // (odTag, carId, trajId, pickUpTime, pickUpLoc)
    val originData = rawData
      .map(t => (0, t._1(0), t._2, t._1(5), "POINT (" + t._1(10) + " " + t._1(11) + ")"))
      .filter(_._5 != "POINT (0 0)").map(_.toString().drop(1).dropRight(1))
    // (odTag, carId, trajId, dropOffTime, dropOffLoc)
    val destinationData = rawData
      .map(t => (1, t._1(0), t._2, t._1(6), "POINT (" + t._1(12) + " " + t._1(13) + ")"))
      .filter(_._5 != "POINT (0 0)").map(_.toString().drop(1).dropRight(1))

    rawData.unpersist()
    (originData, destinationData)
  }

  /**
   * `1-MULTIPOINT Z((X1 Y1 T1), (X2 Y2 T2), ...)` to `LINESTRING (X1 Y1, X2 Y2, ...)`
   */
  def lorrySpatial(data: RDD[String]): RDD[String] =
    data.map(line => {
      val wktInMultiPointZ = line.split("-").last
      val coords =
        wktInMultiPointZ.drop(13).dropRight(1)
          .split(",").map(_.trim).map(t => {
          val xyt = t.drop(1).dropRight(1).split(" ")
          xyt.head + " " + xyt(1)
        }).mkString(", ")
      "LINESTRING (" + coords + ")"
    })

  /**
   * `1-MULTIPOINT Z((X1 Y1 T1), (X2 Y2 T2), ...)` to `LINESTRING (X1 Y1, X2 Y2, ...), T1, TN`
   */
  def lorrySpatialTemporal(data: RDD[String]): RDD[String] =
    data.map(line => {
      val wktInMultiPointZ = line.split("-").last
      val coordsWithTime =
        wktInMultiPointZ.drop(13).dropRight(1)
          .split(",").map(_.trim).map(t => t.drop(1).dropRight(1).split(" "))
      val (startTime, endTime) = (coordsWithTime.head.last, coordsWithTime.last.last)
      "LINESTRING (" + coordsWithTime.map(t => t.head + " " + t(1)).mkString(", ") + ")" + "-" + startTime + "-" + endTime
    })

  /**
   * extract data by given mbr
   */
  def extractByMBR(data: RDD[String], mbr: Envelope): RDD[String] =
    data.filter(p => mbr.contains(WKTUtils.read(p).getEnvelopeInternal))

  def didiToJust(data: RDD[String]): RDD[String] =
    data.map(line => {
      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val List(oid, tid, traj) = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").toList
      val stSeries = "[" + traj.drop(2).dropRight(2).split(", ").map(t => {
        val gps = t.split(" ")
        // [timestamp,lng,lat]
        "[" + Seq("\"" + fm.format(new Date(gps.last.toLong * 1000)) + "\"",
          gps.head.toDouble, gps(1).toDouble).mkString(",") + "]"
      }).mkString(",") + "]"
      oid + "\t" + tid + "\t" + stSeries
    })

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataProcessApp")
      .setMaster("local[*]")
    val spark = new SparkContext(conf)
    val inputPath = DataProcessApp.getClass.getClassLoader.getResource("lorry.txt").getPath
  }
}
