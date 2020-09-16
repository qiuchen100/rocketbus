package io.github.qiuchen100.rocketbus.impl

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.github.qiuchen100.rocketbus.action.HudiAction
import io.github.qiuchen100.rocketbus.api._

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/9/16 13:39
 * @Modified By:
 **/
class HudiOutput(sparkSession : SparkSession, processName: String, processMode: String,
                 format: String, sql: String, dependencies : Array[String], conf: Map[String, String])
  extends AbstractOutput(sparkSession, processName, processMode, format, sql, dependencies, conf) {

  private var _dataFrame: DataFrame = _

  private val hudiAction: HudiAction = new HudiAction

  override def execute(): Unit = {
    _dataFrame = sparkSession.sql(sql)

    if (processMode == "batch") {
      hudiAction.doAction(_dataFrame, conf)
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

      _dataFrame.writeStream
        .trigger(trigger)
        .foreachBatch((batchDF: DataFrame, _: Long)  => {
          hudiAction.doAction(batchDF, conf)
        })
        .option("checkpointLocation", conf("checkpointLocation"))
        .start()
    } else {
      throw new IllegalStateException(s"Wrong Configuration: processMode is wrong [${processMode}], must be batch or stream!")
    }
  }

}

