package io.github.qiuchen100.rocketbus.impl

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.github.qiuchen100.rocketbus.api._

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 16:23
 * @Modified By:
 **/
class HiveOutput(sparkSession : SparkSession, processName: String, processMode: String,
                 format: String, sql: String, dependencies : Array[String], conf: Map[String, String])
  extends AbstractOutput(sparkSession, processName, processMode, format, sql, dependencies, conf) {

  private var _dataFrame: DataFrame = _

  override def execute(): Unit = {

    if (processMode == "batch") {
      conf.filter(cf => !Set("format", "sql").contains(cf._1))
        .foreach(cf => {  //设置写Hive相关参数
          sparkSession.sql(cf._2)
        })
      sparkSession.sql(sql)

    } else if (processMode == "stream") {
      _dataFrame = sparkSession.sql(sql)

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
      {!Set("triggerTime", "triggerType", "format", "outputMode", "saveMode", "destTable").contains(cf._1)})

      _dataFrame.writeStream
        .trigger(trigger)
        .outputMode(conf("outputMode"))
        .options(options)
        .foreachBatch((batchDF: DataFrame, _: Long)  => {
          batchDF.write
            .mode(conf("saveMode"))
            .options(options)
            .insertInto(conf("descTable"))
        })
        .start()

    } else {
      throw new IllegalStateException(s"Wrong Configuration: processMode is wrong [${processMode}], must be batch or stream!")
    }
  }

}
