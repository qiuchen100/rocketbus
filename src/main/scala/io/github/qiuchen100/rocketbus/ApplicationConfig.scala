package io.github.qiuchen100.rocketbus

/**
 * @Author: github.com/qiuchen100
 * @Description: 解析配置文件，并生成"application", "input", "compute", "output"相关配置
 * @param configFilePath 配置文件路径
 * @Date: 2020/3/23 23:49
 * @Modified By:
 **/
class ApplicationConfig(configFilePath: String) {
  private val rootConfig = Configuration(configFilePath)
  rootConfig.assertPathExists("global", "input", "compute", "output")

  private var _globalConfig: Configuration = _

  private var _inputConfigs: Configuration = _

  private var _computeConfigs: Configuration = _

  private var _outputConfigs: Configuration = _

  private var _inputProcessConfigs: Map[String, Configuration] = _

  private var _computeProcessConfigs: Map[String, Configuration] = _

  private var _outputProcessConfigs: Map[String, Configuration] = _


  private[this] def loadAndValidConfig = {

    _globalConfig = rootConfig.getConfig("global")

    _globalConfig.assertValueExists("appName", "appMode")

    _inputConfigs = rootConfig.getConfig("input")

    _computeConfigs = rootConfig.getConfig("compute")

    _outputConfigs = rootConfig.getConfig("output")

    _inputProcessConfigs = getProcessConfigs("input")

    _computeProcessConfigs = getProcessConfigs("compute")

    _outputProcessConfigs = getProcessConfigs("output")
  }

  private[this] def loadAndValidProcessConfig(configType: String, path: String) = {
    val config = configType match {
      case "input" => {
        _inputConfigs.getConfig(path).assertValueExists("format", "processMode")
        _inputConfigs.getConfig(path)
      }
      case "compute" => {
        _computeConfigs.getConfig(path).assertValueExists("sql", "dependencies")
        _computeConfigs.getConfig(path)
      }
      case "output" => {
        _outputConfigs.getConfig(path).assertValueExists("format", "processMode", "sql", "dependencies")
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

  def globalConfig = _globalConfig.getConfigMap()

  def inputProcessConfigs = _inputProcessConfigs
    .map(confs => (confs._1, confs._2.getConfigMap()))

  def computeProcessConfigs = _computeProcessConfigs
    .map(confs => (confs._1, confs._2.getConfigMap()))

  def outputProcessConfigs = _outputProcessConfigs
    .map(confs => (confs._1, confs._2.getConfigMap()))

  loadAndValidConfig

}

object ApplicationConfig extends App {

  def print(): Unit = {
    val applicationConfig = new ApplicationConfig("d://work//ubas_nu_test.conf")
    println("----------global----------------")
    println(applicationConfig.globalConfig)
    println("----------input----------------")
    println(applicationConfig.inputProcessConfigs)
    println("----------compute----------------")
    println(applicationConfig.computeProcessConfigs)
    println("----------output----------------")
    println(applicationConfig.outputProcessConfigs)
  }

  print()

}