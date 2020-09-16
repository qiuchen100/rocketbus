package io.github.qiuchen100.rocketbus.impl

import io.github.qiuchen100.rocketbus.api._
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/9/16 13:39
 * @Modified By:
 **/
class HudiBinlogOutput(sparkSession : SparkSession, processName: String, processMode: String,
                       format: String, sql: String, dependencies : Array[String], conf: Map[String, String])
  extends AbstractOutput(sparkSession, processName, processMode, format, sql, dependencies, conf) {

  private var _dataFrame: DataFrame = _

  override def execute(): Unit = {
    _dataFrame = sparkSession.sql(sql)

   if (processMode == "stream") {
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
      val db = conf("db")
      val tables = conf("tables").split(",")

      val basePath = conf("basePath")

      _dataFrame.writeStream
        .trigger(trigger)
        .foreachBatch((batchDF: DataFrame, _: Long)  => {
          tables.foreach(table => {
            val cols = conf(table)
            val fixedCols = conf("fixedCols")
            val json_cols = cols.split(",")
              .map(col => s"'${col}'")
              .mkString(",")

            batchDF
              .filter(s"table = '${table}'")
              .selectExpr(selectCols(fixedCols, cols) : _*)
              .selectExpr(selectColsWithPartition(fixedCols, cols, conf("partitionCol"), conf("partitionName")) : _*)
              .write
              .format("org.apache.hudi")
              .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, conf("tableType"))
              .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, conf("rowKey"))
              .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, conf("timestamp"))
              .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, conf("partitionName"))
              .option(HoodieWriteConfig.TABLE_NAME, s"${db}_${table}")
              .option("hoodie.insert.shuffle.parallelism", conf("parallelism"))
              .option("hoodie.upsert.shuffle.parallelism", conf("parallelism"))
              .mode(conf("saveMode"))
              .save(s"${basePath}/${db}_${table}")
          })
        })
        .option("checkpointLocation", conf("checkpointLocation"))
        .start()
    } else {
      throw new IllegalStateException(s"Wrong Configuration: processMode is wrong [${processMode}], must be stream!")
    }
  }

  def distributeColsSql(table : String, fixedCols : String, cols : String) = {
    val json_cols = cols.split(",")
      .map(col => s"'${col}'")
      .mkString(",")
    s"""
       |select ${cols}, ${fixedCols}
       |  from ${table} lateral view json_tuple(data, ${json_cols}) data as ${cols}
     """.stripMargin
  }

  def selectCols(fixedCols : String, cols : String) = {
    val selectColArray = scala.collection.mutable.ArrayBuffer[String]()
    selectColArray ++= cols.split(",")
      .map(col => s"get_json_object(data, '$$.${col}') as ${col}")
    selectColArray ++= fixedCols.split(",")
    selectColArray.toArray
  }

  def selectColsWithPartition(fixedCols : String, cols : String, partitionCol : String , partitionName : String) = {
    val selectColArray = scala.collection.mutable.ArrayBuffer[String]()
    selectColArray ++= cols.split(",")
    selectColArray ++= fixedCols.split(",")
    selectColArray += s"${partitionCol} as ${partitionName}"
    selectColArray.toArray
  }

}

