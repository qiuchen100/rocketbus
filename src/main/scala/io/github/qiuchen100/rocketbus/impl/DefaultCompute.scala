package io.github.qiuchen100.rocketbus.impl

import io.github.qiuchen100.rocketbus.api.AbstractCompute
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/28 23:49
 * @Modified By:
 **/
class DefaultCompute(sparkSession : SparkSession, processName: String, conf: Map[String, String])
  extends AbstractCompute(sparkSession, processName, conf) {

  private var _dataFrame : DataFrame = _

  override def execute: Unit = {
    _dataFrame = sparkSession.sql(sql)
    if (isPersist) {
      _dataFrame.persist(StorageLevel.fromString(conf("persist")))
    }
    _dataFrame.createOrReplaceTempView(processName)
  }

}