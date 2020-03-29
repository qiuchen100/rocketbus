package io.github.qiuchen100.rocketbus.api

import org.apache.spark.sql.SparkSession

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 16:23
 * @Modified By:
 **/
abstract class AbstractOutput(sparkSession : SparkSession, processName: String, processMode: String, conf: Map[String, String])
  extends OutputProcess {

  val sql : String = conf(sql)

  def description: String = {
    var result = "{"
    result += "processName : " + this.processName
    result += ", processType : " + this.processType
    result += ", processMode : " + this.processMode
    result += ", conf : " + this.conf.toString
    result = "}"
    result
  }
}
