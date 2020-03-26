package io.github.qiuchen100.rocketbus

/**
 * 解析配置文件，并生成"application", "input", "compute", "output"相关配置
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/23 23:49
 * @Modified By:
 **/
class ApplicationConfig(configFilePath: String) {
  private val rootConfig = Configuration(configFilePath)
  rootConfig.assertPathExists("application", "input", "compute", "output")

  private var _applicationConfig: Configuration = _

  private var _inputConfigs: Configuration = _

  private var _computeConfigs: Configuration = _

  private var _outputConfigs: Configuration = _

  private var _inputProcessConfig: Map[String, Configuration] = _

  private var _computeProcessConfig: Map[String, Configuration] = _

  private var _outputProcessConfig: Map[String, Configuration] = _


  private[this] def loadAndValidConfig = {

    _applicationConfig = rootConfig.getConfig("application")

    _applicationConfig.assertValueExists("appName")

    _inputConfigs = rootConfig.getConfig("input")

    _computeConfigs = rootConfig.getConfig("compute")

    _outputConfigs = rootConfig.getConfig("output")

    _inputProcessConfig = getProcessConfigs("input")

    _computeProcessConfig = getProcessConfigs("compute")

    _outputProcessConfig = getProcessConfigs("output")
  }

  private[this] def loadAndValidProcessConfig(configType: String, path: String) = {
    val config = configType match {
      case "input" => {
        _inputConfigs.getConfig(path).assertValueExists("format", "jobtype")
        _inputConfigs.getConfig(path)
      }
      case "compute" => {
        _computeConfigs.getConfig(path).assertValueExists("sql", "dependencies")
        _computeConfigs.getConfig(path)
      }
      case "output" => {
        _outputConfigs.getConfig(path).assertValueExists("type", "dependencies")
        _outputConfigs.getConfig(path)
      }
    }
    config
  }

  private[this] def getProcessConfigs(configType: String) = {
    rootConfig.getConfigGroupPaths(configType)
      .map(path => (path, loadAndValidProcessConfig(configType, path)))
      .map(pathConfig => (pathConfig._1, pathConfig._2))
      .toMap
  }

  def applicationConfig = _applicationConfig.getConfigMap()

  def inputProcessConfig = _inputProcessConfig
    .map(confs => (confs._1, confs._2.getConfigMap()))

  def computeProcessConfig = _computeProcessConfig
    .map(confs => (confs._1, confs._2.getConfigMap()))

  def outputProcessConfig = _outputProcessConfig
    .map(confs => (confs._1, confs._2.getConfigMap()))

  loadAndValidConfig

}

object ApplicationConfig {

  def print(): Unit = {
    val applicationConfig = new ApplicationConfig("e://application.production.conf")
    println("----------application----------------")
    println(applicationConfig.applicationConfig)
    println("----------input----------------")
    println(applicationConfig.inputProcessConfig)
    println("----------compute----------------")
    println(applicationConfig.computeProcessConfig)
    println("----------output----------------")
    println(applicationConfig.outputProcessConfig)
  }
}