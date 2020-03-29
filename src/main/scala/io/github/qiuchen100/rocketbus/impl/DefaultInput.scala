package com.qihoo.finance.rtos.hudi.impl

import io.github.qiuchen100.rocketbus.api.AbstractInput
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 23:49
 * @Modified By:
 **/
class DefaultInput(sparkSession: SparkSession, processName: String, processMode: String, conf: Map[String, String]) extends
  AbstractInput(sparkSession, processName, processMode, conf) {

  private var _dataFrame: DataFrame = _

  private val _format = conf("format")

  override def execute(): Unit = {
    val options = conf.filter(_._1 != "format")

    if (processMode == "batch") {
      _dataFrame = sparkSession.read
        .format(_format)
        .options(options)
        .load()
      _dataFrame.createOrReplaceTempView(processName)
    } else if (processMode == "stream") {
      _dataFrame = sparkSession.readStream
        .format(_format)
        .options(options)
        .load()
      _dataFrame.createOrReplaceTempView(processName)
    } else {
      throw new IllegalStateException(s"Wrong Configuration: processMode is wrong [${processMode}], must be batch or stream!")
    }
  }

}