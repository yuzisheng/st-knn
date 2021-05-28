package com.urbancomputing.app

import com.urbancomputing.util.WKTUtils
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{LineString, Polygon}

object DataAnalysisApp {
  /**
   * analysis point
   */
  def point(data: RDD[String]): (Long, Long, Double) = {
    val rowCount = data.count()
    (rowCount, rowCount * 1, 1.0)
  }

  /**
   * analysis linestring
   */
  def lineString(data: RDD[String]): (Long, Long, Double) = {
    data.persist()
    val rowCount = data.count()
    val totalPointNum = data
      .map(WKTUtils.read(_).asInstanceOf[LineString])
      .map(_.getNumPoints)
      .reduce(_ + _)
    data.unpersist()
    (rowCount, totalPointNum, totalPointNum.toDouble / rowCount)
  }

  /**
   * analysis polygon
   */
  def polygon(data: RDD[String]): (Long, Long, Double) = {
    data.persist()
    val rowCount = data.count()
    val totalPointNum = data
      .map(WKTUtils.read(_).asInstanceOf[Polygon])
      .map(_.getNumPoints)
      .reduce(_ + _)
    data.unpersist()
    (rowCount, totalPointNum, totalPointNum.toDouble / rowCount)
  }
}
