# MyFlinkProjectTestWithJava
----
参考：[云邪的博客](http://wuchong.me/blog/2018/11/07/use-flink-calculate-hot-items/)
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
** 数据集已经在项目的 resources 目录下。 **
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
这样我们就得到了一个带有时间标记的数据流了，后面就能做一些窗口的操作。
### 过滤出点击事件
在开始窗口操作之前，先回顾下需求“每隔5分钟输出过去一小时内点击量最多的前 N 个商品”。由于原始数据中存在点击、加购、购买、收藏各种行为的数据，但是我们只需要统计点击量，所以先使用 FilterFunction 将点击行为数据过滤出来。
```java
DataStream<UserBehavior> pvData = timedData
    .filter(new FilterFunction<UserBehavior>() {
      @Override
      public boolean filter(UserBehavior userBehavior) throws Exception {
        // 过滤出只有点击的数据
        return userBehavior.behavior.equals("pv");
      }
    });
 ```
 ### 窗口统计点击量
由于要每隔5分钟统计一次最近一小时每个商品的点击量，所以窗口大小是一小时，每隔5分钟滑动一次。即分别要统计 \[09:00, 10:00), \[09:05, 10:05), \[09:10, 10:10)… 等窗口的商品点击量。是一个常见的滑动窗口需求（Sliding Window）。
```java
DataStream<ItemViewCount> windowedData = pvData
    .keyBy("itemId")
    .timeWindow(Time.minutes(60), Time.minutes(5))
    .aggregate(new CountAgg(), new WindowResultFunction());
```
我们使用.keyBy("itemId")对商品进行分组，使用.timeWindow(Time size, Time slide)对每个商品做滑动窗口（1小时窗口，5分钟滑动一次）。然后我们使用 .aggregate(AggregateFunction af, WindowFunction wf) 做增量的聚合操作，它能使用AggregateFunction提前聚合掉数据，减少 state 的存储压力。较之.apply(WindowFunction wf)会将窗口中的数据都存储下来，最后一起计算要高效地多。aggregate()方法的第一个参数用于这里的CountAgg实现了AggregateFunction接口，功能是统计窗口中的条数，即遇到一条数据就加一。
```java
/** COUNT 统计的聚合函数实现，每出现一条记录加一 */
public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

  @Override
  public Long createAccumulator() {
    return 0L;
  }

  @Override
  public Long add(UserBehavior userBehavior, Long acc) {
    return acc + 1;
  }

  @Override
  public Long getResult(Long acc) {
    return acc;
  }

  @Override
  public Long merge(Long acc1, Long acc2) {
    return acc1 + acc2;
  }
}
```
.aggregate(AggregateFunction af, WindowFunction wf) 的第二个参数WindowFunction将每个 key每个窗口聚合后的结果带上其他信息进行输出。我们这里实现的WindowResultFunction将主键商品ID，窗口，点击量封装成了ItemViewCount进行输出。
### TopN 计算最热门商品
为了统计每个窗口下最热门的商品，我们需要再次按窗口进行分组，这里根据ItemViewCount中的windowEnd进行keyBy()操作。然后使用 ProcessFunction 实现一个自定义的 TopN 函数 TopNHotItems 来计算点击量排名前3名的商品，并将排名结果格式化成字符串，便于后续输出。
```java
DataStream<String> topItems = windowedData
    .keyBy("windowEnd")
    .process(new TopNHotItems(3));  // 求点击量前3名的商品
 ```
 ProcessFunction 是 Flink 提供的一个 low-level API，用于实现更高级的功能。它主要提供了定时器 timer 的功能（支持EventTime或ProcessingTime）。本案例中我们将利用 timer 来判断何时收齐了某个 window 下所有商品的点击量数据。由于 Watermark 的进度是全局的，

在 processElement 方法中，每当收到一条数据（ItemViewCount），我们就注册一个 windowEnd+1 的定时器（Flink 框架会自动忽略同一时间的重复注册）。windowEnd+1 的定时器被触发时，意味着收到了windowEnd+1的 Watermark，即收齐了该windowEnd下的所有商品窗口统计值。我们在 onTimer() 中处理将收集的所有商品及点击量进行排序，选出 TopN，并将排名信息格式化成字符串后进行输出。

这里我们还使用了 ListState<ItemViewCount> 来存储收到的每条 ItemViewCount 消息，保证在发生故障时，状态数据的不丢失和一致性。ListState 是 Flink 提供的类似 Java List 接口的 State API，它集成了框架的 checkpoint 机制，自动做到了 exactly-once 的语义保证。
   
### 运行程序
直接运行 main 函数，就能看到不断输出的每个时间点的热门商品ID。
![运行结果](https://img.alicdn.com/tfs/TB1o_fIn3TqK1RjSZPhXXXfOFXa-1534-1270.png)
