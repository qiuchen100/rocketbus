package io.github.qiuchen100.rocketbus

import io.github.qiuchen100.rocketbus.api._
import io.github.qiuchen100.rocketbus.impl._
import org.apache.spark.sql.SparkSession

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/26 23:26
 * @Modified By:
 **/
class ApplicationContext(applicationConfig: ApplicationConfig) {


  def loadStartProcess(conf: Map[String, String]): AbstractStart = {

    val appName: String = conf("appName")
    val appMode: String = conf("appMode")

    new DefaultStart(appName, appMode, conf.filter(cf => !Set("appName", "appMode").contains(cf._1)))
  }

  def loadInputProcess(sparkSession: SparkSession, processName : String, conf: Map[String, String]) : AbstractInput = {
    val processMode = conf("processMode")
    val format = conf("format")

    if (format == "hive") {
      new HiveInput(sparkSession, processName, processMode,
        format, conf.filter(cf => !Set("processName", "processMode", "format").contains(cf._1)))
    } else {
      new DefaultInput(sparkSession, processName, processMode,
        format, conf.filter(cf => !Set("processName", "processMode", "format").contains(cf._1)))
    }
  }

  def loadComputeProcess(sparkSession: SparkSession, processName : String, conf: Map[String, String]) = {
    val sql = conf("sql")
    val isPersist = conf.contains("persist")
    val dependencies = conf("dependencies").split(",")
    new DefaultCompute(sparkSession, processName, sql, isPersist,
      dependencies, conf.filter(cf => !Set("processName", "sql", "persist", "dependencies").contains(cf._1)))
  }

  def loadOutputProcess(sparkSession: SparkSession, processName : String, conf: Map[String, String]) : AbstractOutput = {
    val processMode = conf("processMode")
    val format = conf("format")
    val sql = conf("sql")
    val dependencies = conf("dependencies").split(",")

    if (format == "hive") {
      new HiveOutput(sparkSession, processName, processMode, format, sql,
        dependencies, conf.filter(cf => !Set("processName", "processMode", "format", "sql", "dependencies").contains(cf._1)))
    } else {
      new DefaultOutput(sparkSession, processName, processMode, format, sql,
        dependencies, conf.filter(cf => !Set("processName", "processMode", "format", "sql", "dependencies").contains(cf._1)))
    }
  }

  def loadEndProcess(sparkSession: SparkSession, conf: Map[String, String]): AbstractEnd = {

    val appName: String = conf("appName")
    val appMode: String = conf("appMode")
    val dependencies = conf("dependencies").split(",")
    new DefaultEnd(sparkSession, appName, appMode,
      dependencies, conf.filter(cf => !Set("appName", "appMode", "dependencies").contains(cf._1)))
  }

  def createApplicationModel() : ApplicationModel = {
    var applicationModel = new ApplicationModel
    val globalConfig = applicationConfig.globalConfig
    applicationModel.startProcess = loadStartProcess(globalConfig)
    val sparkSession = applicationModel.startProcess.getSparkSession
    applicationModel.inputProcess = applicationConfig.inputProcessConfigs
      .map(config => loadInputProcess(sparkSession, config._1, config._2))
      .toList
    applicationModel.computeProcess = applicationConfig.computeProcessConfigs
      .map(config => loadComputeProcess(sparkSession, config._1, config._2))
      .toList
    applicationModel.outputProcess = applicationConfig.outputProcessConfigs
      .map(config => loadOutputProcess(sparkSession, config._1, config._2))
      .toList
    applicationModel.endProcess = loadEndProcess(sparkSession, globalConfig)

    applicationModel
  }

}

