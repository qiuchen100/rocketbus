package io.github.qiuchen100.rocketbus.action

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.DataFrame

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/9/16 13:39
 * @Modified By:
 **/
class HudiAction extends AbstractAction {
  override def doAction(df: DataFrame, conf: Map[String, String]): Unit = {
    df.write
      .format("org.apache.hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, conf("tableType"))
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, conf("rowKey"))
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, conf("timestamp"))
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, conf("partition"))
      .option(HoodieWriteConfig.TABLE_NAME, conf("destTable"))
      //            .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option("hoodie.insert.shuffle.parallelism", conf("parallelism"))
      .option("hoodie.upsert.shuffle.parallelism", conf("parallelism"))
      .mode(conf("saveMode"))
      .save(conf("basePath"))
  }
}
