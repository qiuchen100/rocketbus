package io.github.qiuchen100.rocketbus.impl

import io.github.qiuchen100.rocketbus.api.InputProcess
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/23 23:36
 * @Modified By:
 **/
class DefaultInput(sparkSession : SparkSession, name: String, conf: Map[String, String]) extends InputProcess{

  private var _dataFrame : DataFrame = _

  override val processType = "batchInput"

  override def execute(sparkSession : SparkSession, name: String, conf: Map[String, String]): Unit = {
    _dataFrame = sparkSession.read
      .format(conf("format"))
      .options(conf.filter(_._1 != "format"))
      .load()
    _dataFrame.createOrReplaceTempView(name)
  }
}
