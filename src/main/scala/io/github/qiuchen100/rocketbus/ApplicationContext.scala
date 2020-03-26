package io.github.qiuchen100.rocketbus

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/26 23:26
 * @Modified By:
 **/
class ApplicationContext(applicationConfig: ApplicationConfig) {
  val _applicationConfig = applicationConfig

  val _sparkSession = loadSparkSession(_applicationConfig.applicationConfig)

  def loadSparkSession(conf : Map[String, String]): SparkSession = SparkSessionSingleton.getInstance(conf)

  def loadInputProcess(name: String, conf: Map[String, String]) = {
    val jobtype = conf("jobtype")

    if (jobtype == "batch") {
      new DefaultInput(_sparkSession, name, conf)
    } else if(jobtype == "batch") {
      new DefaultStreamInput(_sparkSession, name, conf)
    } else {
      throw new IllegalStateException(s"Wrong Configuration: jobtype must be batch or stream!")
    }
  }

  def loadComputeProcess(name: String, conf: Map[String, String]) = new DefaultCompute(_sparkSession, name, conf)


  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _
    def getInstance(conf : Map[String, String]): SparkSession = {
      val sparkConf = new SparkConf()
      val warehouseLocation = "/user/hive/warehouse/"
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .appName(conf("appName"))
          .config("spark.sql.warehouse.dir", conf.getOrElse("warehouse.dir", warehouseLocation))
          .config("spark.serializer",conf.getOrElse("warehouse.dir", "org.apache.spark.serializer.KryoSerializer"))
          .config("hive.exec.dynamic.partition", true)
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .enableHiveSupport()
          .getOrCreate()
      }
      instance
    }
  }
}

