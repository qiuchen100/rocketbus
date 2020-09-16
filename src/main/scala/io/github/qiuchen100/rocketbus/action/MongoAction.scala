package io.github.qiuchen100.rocketbus.action

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.DataFrame

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/9/16 13:39
 * @Modified By:
 **/
class MongoAction extends AbstractAction {
  override def doAction(df: DataFrame, conf: Map[String, String]): Unit = {
    val writeConfig = WriteConfig(conf("databaseName"), conf("collectionName"))
    MongoSpark.save(df, writeConfig.withOptions(conf))
  }
}
