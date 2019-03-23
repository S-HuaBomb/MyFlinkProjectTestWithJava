package MyFlinkProject;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotItems {
    public static void main(String[] args) throws Exception {

        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Flink 默认使用 ProcessingTime处理，所以我们要显式设置下。告诉系统按照EventTime处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，改变并发对结果正确性没有影响
        env.setParallelism(1);

        /**
         * 创建一个PojoCsvInputFormat
         * 这是一个读取 csv 文件并将每一行转成指定 POJO 类型（在我们案例中是UserBehavior）的输入器。
         * 下面filePath是UserBehavior.csv的本地文件路径
         */
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        // 抽取UserBehavior 的 TypeInformation，是一个PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>)
                TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于Java反射抽取出的字段顺序是不确定的，需要显式指定文件中字段的顺序
        String[] fileOrder = new String[] {"userId", "itemId", "categoryId", "behavior",
                "timestamp"};
        // 创建PojoCSVInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType,
                fileOrder);

        // 用PojoCsvInputFormat 创建输入源,一个UserBehavior 类型的DataStream
        DataStream<UserBehavior> dataSource = env.createInput(csvInput, pojoType);

        /*
        用AscendingTimestampExtractor 来实现时间戳的抽取和 Watermark 的生成。
        注： 真实业务场景一般都是存在乱序的，所以一般使用BoundedOutOfOrdernessTimestampExtractor。
        这样我们就得到了一个带有时间标记的数据流了，后面就能做一些窗口的操作。
        */
        DataStream<UserBehavior> timeData = dataSource.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.timestamp * 1000;
                    }
                });

        // 先使用FilterTunction将点击行为数据过滤出来
        DataStream<UserBehavior> pvData = timeData
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {
                        return userBehavior.behavior.equals("pv");
                    }
                });

        // 窗口大小是一小时，每隔5 分钟滑动一次
        DataStream<ItemViewCount> windowData = pvData
                .keyBy("itemId")    // 对商品ID进行分组
                .timeWindow(Time.minutes(60), Time.minutes(5))  // 滑动窗口
                .aggregate(new CountAgg(), new WindowResultFunction()); // 增量的聚合操作，减少state的存储压力

        DataStream<String> topItems = windowData
                .keyBy("windowEnd")
                .process(new TopNHotItems(3));

        topItems.print();

        env.execute("Hot Items Job");
    }

    /* ListState 是 Flink 提供的类似 Java List 接口的State API，它集成了框架的 checkpoint 机制，
     自动做到了 exactly-once 的语义保证。
    以下为：求某个窗口中前N名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
    */
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        private final int topSize;
        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }
        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private ListState<ItemViewCount> itemState;
        @Override
        public void open(Configuration paramaters) throws Exception {
            super.open(paramaters);
            // 状态的注册
            ListStateDescriptor<ItemViewCount> itemViewCountListStateDescriptor = new
                    ListStateDescriptor<>("itemState-State", ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemViewCountListStateDescriptor);
        }
        @Override
        public void processElement(
                ItemViewCount input,
                Context context,
                org.apache.flink.util.Collector<String> collector) throws Exception {
            // 每条数据都保存到状态中
            itemState.add(input);
            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd 窗口的所有商品数据
            context.timerService().registerEventTimeTimer(input.windowEnd + 1);
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });
            // 将排名信息格式化成 String，便于打印
            StringBuilder result = new StringBuilder();
            result.append("=========================================\n");
            result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i=0; i<topSize; i++) {
                ItemViewCount currentItem = allItems.get(i);
                // No1: 商品ID=12224 浏览量=2413
                result.append("No").append(i).append(":")
                        .append("   商品ID=").append(currentItem.itemId)
                        .append("   浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("=========================================\n");

            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000);

            out.collect(result.toString());
        }
    }

    /** WindowResultFunction将主键商品ID，窗口，点击量封装成了ItemViewCount 进行输出 */
    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount,
            Tuple, TimeWindow> {
        @Override
        public void apply(
                Tuple key,  // 窗口的主键，即itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 的值
                Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = aggregateResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

    /** 实现了AggregateFunction 接口，功能是统计窗口中的条数。每出现一条记录加1 */
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return  0L;
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

    /** 窗口点击量（窗口操作的输出类型）*/
    public static class ItemViewCount {
        public long itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long viewCount;  // 商品的点击量
        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }

    /**
     创建一个UserBehavior 的 POJO 类（所有成员变量声明成public 便是POJO 类），强类
     型化后能方便后续的处理。以下为用户行为数据中五个属性的类型
     */
    public static class UserBehavior {
        public long userId;
        public long itemId;
        public int categoryId;
        public String behavior;
        public long timestamp;
    }
}
