package io.github.qiuchen100.rocketbus.impl

import org.apache.spark.sql.SparkSession
import io.github.qiuchen100.rocketbus.api._

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/4/8 23:00
 * @Modified By:
 **/
class DefaultEnd(sparkSession : SparkSession, appName: String, appMode: String,
                 conf: Map[String, String])
  extends AbstractEnd(sparkSession, appName, appMode, conf){
  override def execute(): Unit = {
    if (appMode == "batch") {
      sparkSession.stop()
    } else if (appMode == "stream") {
      sparkSession.streams.awaitAnyTermination()
    } else {
      throw new IllegalStateException(s"Wrong Configuration: appMode is wrong [${appMode}], must be batch or stream!")
    }
  }
}
