package io.github.qiuchen100.rocketbus.api


/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/23 23:29
 * @Modified By:
 **/
trait Process {
  def getProcessName : String

  val processType : String

  def getDependencies : Array[String]

  def execute(): Unit

  def description: String
}

