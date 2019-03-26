# MyFlinkProjectTestWithJava
Flink的一个简单应用——计算实时热门商品
### 通过本文你将学到：
   1. 如何基于 EventTime 处理，如何指定 Watermark
   2. 如何使用 Flink 灵活的 Window API
   3. 何时需要用到 State，以及如何使用
   4. 如何使用 ProcessFunction 实现 TopN 功能
   
#### 实战案例介绍
本案例将实现一个“实时热门商品”的需求，我们可以将“实时热门商品”翻译成程序员更好理解的需求：每隔5分钟输出最近一小时内点击量最多的前 N 个商品。将这个需求进行分解我们大概要做这么几件事情：
   * 抽取出业务时间戳，告诉 Flink 框架基于业务时间做窗口
   * 过滤出点击行为数据
   * 按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window）
   * 按每个窗口聚合，输出每个窗口中点击量前N名的商品
### 数据准备
这里我们准备了一份淘宝用户行为数据集（来自[阿里云天池公开数据集](https://tianchi.aliyun.com/dataset/)，特别感谢）。本数据集包含了淘宝上某一天随机一百万用户的所有行为（包括点击、购买、加购、收藏）。数据集的组织形式和MovieLens-20M类似，即数据集的每一行表示一条用户行为，由用户ID、商品ID、商品类目ID、行为类型和时间戳组成，并以逗号分隔。关于数据集中每一列的详细描述如下：

列名称|说明
:--|:--
用户ID|整数类型，加密后的用户ID
商品ID|整数类型，加密后的商品ID
商品类目ID|整数类型，加密后的商品所属类目ID
行为类型|字符串，枚举类型，包括(‘pv’, ‘buy’, ‘cart’, ‘fav’)
时间戳|行为发生的时间戳，单位秒
* 数据集已经在项目的 resources 目录下。 *
### 创建模拟数据源
由于是一个csv文件，我们在程序中使用 CsvInputFormat 创建模拟数据源。
> 注：虽然一个流式应用应该是一个一直运行着的程序，需要消费一个无限数据源。但是在本案例教程中，为了省去构建真实数据源的繁琐，我们使用了文件来模拟真实数据源，这并不影响下文要介绍的知识点。这也是一种本地验证 Flink 应用程序正确性的常用方式。

### EventTime 与 Watermark
当我们说“统计过去一小时内点击量”，这里的“一小时”是指什么呢？ 在 Flink 中它可以是指 ProcessingTime ，也可以是 EventTime，由用户决定。
   * ProcessingTime：事件被处理的时间。也就是由机器的系统时间来决定。
   * EventTime：事件发生的时间。一般就是数据本身携带的时间。
   
在本案例中，我们需要统计业务时间上的每小时的点击量，所以要基于 EventTime 来处理。那么如果让 Flink 按照我们想要的业务时间来处理呢？这里主要有两件事情要做。
第一件是告诉 Flink 我们现在按照 EventTime 模式进行处理，Flink 默认使用 ProcessingTime 处理，所以我们要显式设置下。

` env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); `

第二件事情是指定如何获得业务时间，以及生成 Watermark。Watermark 是用来追踪业务事件的概念，可以理解成 EventTime 世界中的时钟，用来指示当前处理到什么时刻的数据了。由于我们的数据源的数据已经经过整理，没有乱序，即事件的时间戳是单调递增的，所以可以将每条数据的业务时间就当做 Watermark。这里我们用  AscendingTimestampExtractor 来实现时间戳的抽取和 Watermark 的生成。
> 注：真实业务场景一般都是存在乱序的，所以一般使用 BoundedOutOfOrdernessTimestampExtractor。
``` java
DataStream<UserBehavior> timedData = dataSource
    .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
      @Override
      public long extractAscendingTimestamp(UserBehavior userBehavior) {
        // 原始数据单位秒，将其转成毫秒
        return userBehavior.timestamp * 1000;
      }
    });
 ```


