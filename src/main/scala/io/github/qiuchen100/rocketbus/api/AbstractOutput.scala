package io.github.qiuchen100.rocketbus.api

import org.apache.spark.sql.SparkSession

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 16:23
 * @Modified By:
 **/
abstract class AbstractOutput(sparkSession : SparkSession, processName: String, processMode: String,
                              format: String, sql: String, dependencies : Array[String], conf: Map[String, String])
  extends OutputProcess {

  def getProcessName: String = processName

  def description: String = {
    var result = "{"
    result += "processName : " + this.processName
    result += ", processType : " + this.processType
    result += ", processMode : " + this.processMode
    result += ", format : " + this.format
    result += ", sql : " + this.sql
    result += ", conf : " + this.conf.toString
    result = "}"
    result
  }

  def getDependencies = this.dependencies
}
