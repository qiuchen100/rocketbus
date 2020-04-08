package io.github.qiuchen100.rocketbus.api

import org.apache.spark.sql.SparkSession

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/4/8 23:00
 * @Modified By:
 **/
abstract class AbstractEnd(sparkSession : SparkSession, appName: String, appMode: String,
                           dependencies : Array[String], conf: Map[String, String])
  extends OutputProcess {

  val processName: String = "end"

  def getProcessName: String = processName

  def description: String = {
    var result = "{"
    result += "appName : " + this.appName
    result += ", appMode : " + this.appMode
    result += ", processType : " + this.processType
    result += ", conf : " + this.conf.toString
    result = "}"
    result
  }

  def getDependencies : Array[String] = this.dependencies
}
