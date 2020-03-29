package io.github.qiuchen100.rocketbus.impl

import io.github.qiuchen100.rocketbus.api.AbstractOutput
import org.apache.spark.sql.SparkSession

class HiveBatchOutput(sparkSession: SparkSession, processName: String, processMode: String, conf: Map[String, String])
  extends AbstractOutput(sparkSession, processName, processMode, conf) {

  override def execute(): Unit = {

    conf.filter(cf => !Set("format", "sql").contains(cf._1))
      .foreach(cf => {
        sparkSession.sql(cf._2)
      })

    sparkSession.sql(sql)
  }
}
