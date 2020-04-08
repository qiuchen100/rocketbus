package io.github.qiuchen100.rocketbus.api

import org.apache.spark.sql.SparkSession

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 16:23
 * @Modified By: 
 **/
abstract class AbstractCompute(sparkSession : SparkSession, processName: String,
                               sql : String, isPersist : Boolean, dependencies : Array[String], conf: Map[String, String])
  extends ComputeProcess{

  def getProcessName: String = processName

  def description: String = {
    var result = "{"
    result += "processName : " + this.processName
    result += ", processType : " + this.processType
    result += (if (isPersist) ", persist : " + conf("persist") else "")
    result += ", sql : " + this.sql
    result += ", conf : " + this.conf.toString
    result = "}"
    result
  }

  def getDependencies: Array[String] = this.dependencies
}

