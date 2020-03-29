package io.github.qiuchen100.rocketbus.api

import org.apache.spark.sql.SparkSession

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 16:23
 * @Modified By:
 **/
abstract class AbstractInput(sparkSession : SparkSession, processName: String, processMode: String, conf: Map[String, String])
  extends InputProcess{

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
