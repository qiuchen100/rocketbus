package io.github.qiuchen100.rocketbus.action

import org.apache.spark.sql.DataFrame

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/9/16 13:39
 * @Modified By:
 **/
abstract class AbstractAction {
  def doAction(df: DataFrame, conf: Map[String, String]) : Unit
}
