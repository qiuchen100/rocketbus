package io.github.qiuchen100.rocketbus.impl

import io.github.qiuchen100.rocketbus.api.AbstractOutput
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

class MultiStreamOutput(sparkSession: SparkSession, processName: String, processMode: String, conf: Map[String, String] )
  extends AbstractOutput(sparkSession, processName, processMode, conf) {
  private var _dataFrame: DataFrame = _

//  private val _format = conf("format")

  override def execute(): Unit = {
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
    val query = _dataFrame.writeStream
      .trigger(trigger)
      .outputMode(conf("outputMode"))
      .options(options)
      .foreachBatch((batchDF: DataFrame, _: Long)  => {
        batchDF.write
          .mode(conf("saveMode"))
          .insertInto(conf("descTable"))
      })
      .start()
    query.awaitTermination()
  }
}
