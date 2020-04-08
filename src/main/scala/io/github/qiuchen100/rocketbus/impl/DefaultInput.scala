package io.github.qiuchen100.rocketbus.impl

import io.github.qiuchen100.rocketbus.api.AbstractInput
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 23:49
 * @Modified By:
 **/
class DefaultInput(sparkSession : SparkSession, processName: String, processMode: String,
                   format: String, conf: Map[String, String])
  extends AbstractInput(sparkSession, processName, processMode, format, conf) {

  private var _dataFrame: DataFrame = _

  def execute(): Unit = {

    if (processMode == "batch") {
      _dataFrame = sparkSession.read
        .format(format)
        .options(conf)
        .load()
      _dataFrame.createOrReplaceTempView(processName)
    } else if (processMode == "stream") {
      _dataFrame = sparkSession.readStream
        .format(format)
        .options(conf)
        .load()
      _dataFrame.createOrReplaceTempView(processName)
    } else {
      throw new IllegalStateException(s"Wrong Configuration: processMode is wrong [${processMode}], must be batch or stream!")
    }
  }

}
