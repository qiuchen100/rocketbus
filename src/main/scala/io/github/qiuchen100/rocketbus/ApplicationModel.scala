package io.github.qiuchen100.rocketbus

import io.github.qiuchen100.rocketbus.api._

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/24 0:08
 * @Modified By:
 **/
class ApplicationModel(applicationConfig: ApplicationConfig) {
  val InputProcessGroup = Map[String, InputProcess]
  val ComputeProcessGroup = Map[String, ComputeProcess]
  val OutputProcessGroup = Map[String, OutputProcess]
}

object ApplicationModel {
  def Apply(applicationConfig: ApplicationConfig) ={
    new ApplicationModel(applicationConfig)
  }
}
