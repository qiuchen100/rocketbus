package io.github.qiuchen100.rocketbus

import java.io.File
import com.typesafe.config.{Config, ConfigFactory, ConfigResolveOptions}
import org.apache.commons.lang3.StringUtils
import scala.collection.JavaConverters._

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/26 23:26
 * @Modified By:
 **/
class Configuration(name : String, val config: Config) {

  def getConfigMap() = {
    config.entrySet()
      .asScala
      .map(conf => (conf.getKey, conf.getValue.unwrapped().toString))
      .toMap
  }

  def getConfigGroupPaths(path: String) = {
    val paths = config.getObject(path)
      .entrySet()
      .asScala
      .filter(entry => entry.getValue.valueType.toString == "OBJECT")
      .map(_.getKey)
    if (paths.size < 1) {
      throw new IllegalStateException(s"Wrong Configuration: PATH ${path} is empty!")
    }
    paths
  }

  def getConfig(path : String) = {
    new Configuration(path, config.getConfig(path))
  }

  def getStringValue(key:String, defaultValue:String="") = {
    val value = config.getString(key)
    if(StringUtils.isNotEmpty(value)) {
      value
    } else {
      defaultValue
    }
  }


  def assertPathExists(path : String) = {
    if (!config.hasPath(path) || config.getValue(path).valueType().toString != "OBJECT")
      throw new IllegalStateException(s"Wrong Configuration: PATH ${path} is not in ${name}!")
    if (config.getConfig(path).isEmpty)
      throw new IllegalStateException(s"Wrong Configuration: PATH ${path} is empty!")
  }

  def assertPathExists(paths : String*) = {
    paths.foreach(path => {
      if (!config.hasPath(path) || config.getValue(path).valueType().toString != "OBJECT")
        throw new IllegalStateException(s"Wrong Configuration: PATH ${path} is not in ${name}!")
      if (config.getConfig(path).isEmpty)
        throw new IllegalStateException(s"Wrong Configuration: PATH ${path} is empty!")
    })
  }

  def assertValueExists(path : String) = {
    if (!config.hasPath(path) || config.getValue(path).valueType().toString != "STRING")
      throw new IllegalStateException(s"Wrong Configuration: Value [${path}] is not in ${name}!")
  }

  def assertValueExists(paths : String*) = {
    paths.foreach(path => {
      if (!config.hasPath(path) || config.getValue(path).valueType().toString != "STRING")
        throw new IllegalStateException(s"Wrong Configuration: Value [${path}] is not in ${name}!")
    })
  }
}

object Configuration {

  def apply(configFilePath: String) = {
    val config = ConfigFactory
      .parseFile(new File(configFilePath))
      .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
      .resolveWith(ConfigFactory.systemProperties, ConfigResolveOptions.defaults.setAllowUnresolved(true))
    new Configuration("root", config)
  }

  def main(args: Array[String]): Unit = {
    val conf = Configuration("d://work//ubas_nu_test.conf")
    //    println(conf.getStringValue("spark.spark1.master"))
    //    val sparkConf = conf.getConfig("spark")
    //    println(sparkConf.getStringValue("spark1.master"))
    //    //    conf.load.entrySet().asScala.foreach(entry => {
    //    //      println(entry.getKey + " : " + entry.getValue.unwrapped())
    //    //    })
    //    println(conf.config.hasPath("spark.spark1.master"))
    //    println(conf.config.withOnlyPath("spark.spark1"))
    //    conf.config.getObject("spark")
    //      .entrySet()
    //      .asScala
    //      .foreach(entry=>println(entry.getKey, entry.getValue.valueType()))
    //
    //    conf.assertPathExists("spark.spark1")
    //    //    conf.assertValueExists("spark.spark1.ui.port")
    //    println(conf.config.getValue("spark.spark1.ui.port").valueType()) //NUMBER
    //    conf.assertPathExists("spark.spark2")
    //    conf.assertValueExists("spark.spark3")
    println(conf.getConfigGroupPaths("input"))
  }

}
