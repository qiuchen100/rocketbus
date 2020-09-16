package io.github.qiuchen100.rocketbus.impl

import io.github.qiuchen100.rocketbus.api.AbstractOutput
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 23:49
 * @Modified By:
 **/
class DefaultOutput(sparkSession : SparkSession, processName: String, processMode: String,
                    format: String, sql: String, dependencies : Array[String], conf: Map[String, String])
  extends AbstractOutput(sparkSession, processName, processMode, format, sql, dependencies, conf) {

  private var _dataFrame: DataFrame = _

  override def execute(): Unit = {
    _dataFrame = sparkSession.sql(sql)

    if (processMode == "batch") {
      val options = conf.filter(cf => !Set("saveMode", "format").contains(cf._1))
      _dataFrame.write
        .format(format)
        .mode(conf("saveMode"))
        .options(options)
        .save()
    } else if (processMode == "stream") {
      val triggerType = conf("triggerType")
      val triggerTime = conf("triggerTime")
      val trigger = if(triggerType == "processingTime") {
        Trigger.ProcessingTime(triggerTime)
      } else if(triggerType == "continuous") {
        Trigger.Continuous(triggerTime)
      } else {
        Trigger.ProcessingTime("10 seconds")
      }

      val options = conf.filter(cf =>
      {!Set("triggerTime", "triggerType", "format", "outputMode").contains(cf._1)})
      val query = _dataFrame.writeStream
        .format(format)
        .trigger(trigger)
        .outputMode(conf("outputMode"))
        .options(options)
        .start()
    } else {
      throw new IllegalStateException(s"Wrong Configuration: processMode is wrong [${processMode}], must be batch or stream!")
    }
  }

}
