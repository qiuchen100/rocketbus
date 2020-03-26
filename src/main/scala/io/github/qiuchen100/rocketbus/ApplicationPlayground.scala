package io.github.qiuchen100.rocketbus

/**
 * @Author: github.com/qiuchen100
 * @Description: TODO
 * @Date: 2020/3/26 23:26
 * @Modified By:
 **/
object ApplicationPlayground {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        """Usage: ApplicationPlayground <configFile>
        """.stripMargin)
      System.exit(1)
    }

    val applicationConfig  = new ApplicationConfig(args(0))

  }
}
