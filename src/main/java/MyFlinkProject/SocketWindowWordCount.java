package MyFlinkProject;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {

        /* Flink 程序的第一步是创建一个StreamExecutionEnvironment。
         * 这是一个入口类，可以用来设置参数和创建数据源以及提交任务。
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
        创建一个从本地端口号 9000 的 socket 中读取数据的数据源：这创建了一个字符串类型的DataStream。DataStream
        是 Flink 中做流处理的核心 API，上面定义了非常多常见的操作（如，过滤、转换、聚合、窗口、关联等）
         */
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");
        // 解析数据，按 word 分组，开窗，聚合
        DataStream<Tuple2<String, Integer>> windowCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // 将结果打印到控制台，注意这里使用的是单线程打印，而非多线程
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }
}
