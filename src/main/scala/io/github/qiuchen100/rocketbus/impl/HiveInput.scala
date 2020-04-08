package io.github.qiuchen100.rocketbus.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import io.github.qiuchen100.rocketbus.api._

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 16:23
 * @Modified By:
 **/
class HiveInput(sparkSession : SparkSession, processName: String, processMode: String,
                format: String, conf: Map[String, String]) extends
  AbstractInput(sparkSession, processName, processMode, format, conf) {

  private var _dataFrame: DataFrame = _

  override def execute(): Unit = {

    if (processMode == "batch") {
      _dataFrame = sparkSession.read
        .table(conf("tableName"))

      _dataFrame.createOrReplaceTempView(processName)
    } else {
      throw new IllegalStateException(s"Wrong Configuration: Hive processMode only support batch mode!")
    }
  }

}
