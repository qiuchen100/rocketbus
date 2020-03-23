# RocketBus

#### 介绍
RocketBus是一款基于Spark Sql/Spark Structured Streaming的ETL开发工具。它利用了Spark Sql/Spark Structured Streaming将一切包括batch source与streaming source在内的输入抽象为统一的DataFrame，在DataFrame基础上可以做到SQl化的方式来处理数据源,并对最终的输出也实现了统一的接口这一机制，实现了一款业务流程与程序实现完全分离的，配置优先于开发的，满足大数据平台ETL开发功能的工具。它有以下优点：
1.  通过配置文件即可实现Spark批处理和流处理作业，这种方案不仅能够不需要开发就能配制出一个Spark SQL作业一个或者Spark力计算作业，同时业务流程与程序实现完全分离后，可以让开发者更专注业务本身，不必面对功能代码穿插着业务SQL这种冗长，易错乱的代码。
2.  集成了常用的批处理/流处理InputProcess和OutputProcess，如Kafka、Redis、File、HDFS、Mysql等。这些内置的实现已完全能够满足大部分业务场景，同时还提供了实现自定义的InputProcess和OutputProcess接口供扩展。
3.  舍弃了之前更底层的Spark Rdd编程模型，而完全依托Spark Sql编程模型，虽然让RocketBus必须运行在新版本的Spark上（推荐Spark2.3.0以上），但也让这款工具更加轻便，不必因为兼容旧的版本而做的太复杂，且未充分利用Spark Sql编程模型提供统一的，性能更好的API。

#### 软件架构
软件架构说明


#### 安装教程

1.  xxxx
2.  xxxx
3.  xxxx

#### 使用说明

1.  xxxx
2.  xxxx
3.  xxxx

#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request


#### 码云特技

1.  使用 Readme\_XXX.md 来支持不同的语言，例如 Readme\_en.md, Readme\_zh.md
2.  码云官方博客 [blog.gitee.com](https://blog.gitee.com)
3.  你可以 [https://gitee.com/explore](https://gitee.com/explore) 这个地址来了解码云上的优秀开源项目
4.  [GVP](https://gitee.com/gvp) 全称是码云最有价值开源项目，是码云综合评定出的优秀开源项目
5.  码云官方提供的使用手册 [https://gitee.com/help](https://gitee.com/help)
6.  码云封面人物是一档用来展示码云会员风采的栏目 [https://gitee.com/gitee-stars/](https://gitee.com/gitee-stars/)
