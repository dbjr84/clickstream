package com.dbjr.prac.processes

import com.dbjr.prac.utils.VideoRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.FunSuite

import scala.io.Source
/**
 * Created by debjyoti on 05/15/17.
 * Email - debjyoti.roy@gmail.com
 */
class TestVideoClickStreamProcess extends FunSuite with LocalSparkContext{

  test("testDistinctAndCumulativeTimes"){
    val path = "data/testdata.txt"
    val input1: Seq[String] = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(path)).mkString("").split("\n").toSeq
    val rdd = sc.parallelize(input1)
    val hiveContext = new  SQLContext(sc)

    val dff: DataFrame = hiveContext.read.json(rdd).dropDuplicates()
    val modDFF = dff.withColumn("playId", concat(col("videoId"), lit("&"), col("accountId"), lit("&"), col("userId"))).orderBy("deviceTimestamp")
    var totalDistinctTime = 0L
    var totalCumulativeTime = 0L
    val resRDD: RDD[Row] = modDFF.rdd
    resRDD.groupBy((row: Row) => {
      row.getAs[String]("playId")
    }).collect.map((t: (String, Iterable[Row])) => {

      val sortedIterable: List[Row] = t._2.toList.sortBy(_.getAs[Long]("deviceTimestamp"))
      val playId = t._1
      val assetProcessor = TimeProcessor()
      val result:VideoRecord= assetProcessor.processSortedRDD(sortedIterable, playId)

      totalCumulativeTime = result.cumulativeViewTime
      totalDistinctTime = result.distinctViewedTime

    })

    assert(totalCumulativeTime == 2720000)
    assert(totalDistinctTime == 2700000)
  }

}
