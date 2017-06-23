package com.dbjr.prac.processes

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Suite, BeforeAndAfterAll}

/**
 * Created by debjyoti on 05/15/17.
 * Email - debjyoti.roy@gmail.com
 */
trait LocalSparkContext extends BeforeAndAfterAll  { self: Suite =>
  @transient var sc: SparkContext = _

  override def beforeAll() {
    super.beforeAll()
    val conf = new SparkConf()
    conf.set("spark.master", "local")
    conf.set("spark.ui.port", "40000")
    conf.set("spark.app.name", "testapp")
    sc = new SparkContext(conf)
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }
}
