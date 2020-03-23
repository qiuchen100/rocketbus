# RocketBus

#### 介绍
RocketBus是一款基于Spark Sql/Spark Structured Streaming的ETL开发工具。它利用了Spark Sql/Spark Structured Streaming将一切包括batch source与streaming source在内的输入抽象为统一的DataFrame，在DataFrame基础上可以做到SQl化的方式来处理数据源,并对最终的输出也实现了统一的接口这一机制，实现了一款业务流程与程序逻辑完全分离的，配置优先于实现的，满足大数据平台ETL开发功能的工具。它有以下优点：
1.  通过配置文件即可实现Spark批处理和流处理作业，无需开发Spark代码。
2.  集成了常用的InputProcess和OutputProcess实现，如Kafka、Redis、File、HDFS、Mysql等。这些内置的Process实现已完全能够满足大部分业务场景，同时用户可以实现自定义的InputProcess和OutputProcess，轻松地加载到RocketBus作业中。
3.  舍弃了更底层的Spark Rdd编程模型，而完全以Spark Sql编程模型为基础，虽然让RocketBus必须运行在新版本的Spark上（推荐Spark2.3.0以上），但也让这款工具更加轻便，不必因为兼容旧的版本而设计的太复杂，对第三方接口实现也会更加优化，因为仅需面向Spark Sql编程模型来开发。

#### 设计哲学
1.  **约定优先于配置，配置优先于实现。** RocketBus作业配置参数完全依照Spark作业配置参数，且提供了管理维护作业所必须要的参数设置。
2.  **一个配置文件对应一个作业。** 用户想借助RocketBus配置一个作业，仅需按照约定提供一个配置文件，并按照相应的命令启动，同时传入配置文件
的所在路径即可。
3.  **一个作业的基本单位是Process（流程）。**在RocketBus的世界里，最基本的单位是Process。Process分为InputProcess、ComputeProcess、和OutputProcess三种。一个作业是由至少一个InputProcess，至少一个ComputeProcess和至少一个OutputProcess所构成。这些Process的彼此依赖关系共同构成一个作业的DAG图。


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
