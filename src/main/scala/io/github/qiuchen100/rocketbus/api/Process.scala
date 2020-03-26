package io.github.qiuchen100.rocketbus.api

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/23 23:29
 * @Modified By:
 **/
trait Process extends Logging {
  def execute(sparkSession : SparkSession, name: String, conf: Map[String, String]) : Unit

}
