package io.github.qiuchen100.rocketbus.impl

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import io.github.qiuchen100.rocketbus.api._

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/4/8 23:00
 * @Modified By:
 **/
class DefaultStart(appName: String, appMode: String, conf: Map[String, String])
  extends AbstractStart(appName, appMode, conf){
  override var _sparkSession: SparkSession = _

  override def execute(): Unit = {
    _sparkSession = SparkSessionSingleton.getInstance(appName, conf)
  }

  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _
    def getInstance(appName : String, conf : Map[String, String]): SparkSession = {
      val sparkConf = new SparkConf().setAll(conf)
      //      val warehouseLocation = "/user/hive/warehouse/"
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .appName(appName)
//          .config("spark.sql.warehouse.dir", conf.getOrElse("warehouse.dir", warehouseLocation))
//          .config("spark.serializer",conf.getOrElse("warehouse.dir", "org.apache.spark.serializer.KryoSerializer"))
//          .config("hive.exec.dynamic.partition", true)
//          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .enableHiveSupport()
          .getOrCreate()
      }
      instance
    }
  }
}
