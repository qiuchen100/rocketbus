package io.github.qiuchen100.rocketbus.api

import org.apache.spark.sql.SparkSession

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 16:23
 * @Modified By:
 **/
abstract class AbstractStart(appName: String, appMode: String, conf: Map[String, String])
  extends StartProcess{

  val processName: String = "start"

  def getProcessName: String = processName

  var _sparkSession : SparkSession

  def getSparkSession : SparkSession = _sparkSession


  def description: String = {
    var result = "{"
    result += "appName : " + this.appName
    result += ", appMode : " + this.appMode
    result += ", processType : " + this.processType
    result += ", conf : " + this.conf.toString
    result = "}"
    result
  }

  def getDependencies : Array[String] = Nil.toArray
}
