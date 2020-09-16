package io.github.qiuchen100.rocketbus.action

import org.apache.spark.sql.DataFrame

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/9/16 13:39
 * @Modified By:
 **/
class ESAction extends AbstractAction {
  override def doAction(df: DataFrame, conf: Map[String, String]): Unit = {
    import org.elasticsearch.spark.sql._
    df.saveToEs(conf("ES_INDEX")+"/"+conf("ES_TYPE"), conf)
  }
}
