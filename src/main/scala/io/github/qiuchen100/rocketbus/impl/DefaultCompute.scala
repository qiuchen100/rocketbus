package io.github.qiuchen100.rocketbus.impl

import io.github.qiuchen100.rocketbus.api.ComputeProcess
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/23 23:36
 * @Modified By:
 **/
class DefaultCompute(sparkSession: SparkSession, name: String, conf: Map[String, String]) extends ComputeProcess{

  private var _dataFrame : DataFrame = _

  override def execute(sparkSession: SparkSession, name: String, conf: Map[String, String]): Unit = {
    val sql = conf("sql")
    _dataFrame = sparkSession.sql(sql)
    _dataFrame.createOrReplaceTempView(name)
    val isPersist = conf.getOrElse("persist", "false")
    if (isPersist == "true") {
      _dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
  }
}
