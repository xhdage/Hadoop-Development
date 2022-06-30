package com.xhdage.hotitems_analysis;

import com.xhdage.hotitems_analysis.beans.ItemViewCount;
import com.xhdage.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

public class HotItems {
    public static void main(String[] args) throws Exception {
        // 1、创建环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2、读取数据，创建DataStream

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer");
        // 下面是一些次要参数
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // DataStream<String> inputStream = environment.readTextFile("D:\\开发\\Java\\bigdata\\flink\\UserBehaviorAnalysis\\HotitemsAnalysis\\src\\main\\resources\\UserBehavior.csv");
        // Kafka读取数据
        DataStreamSource<String> inputStream = environment.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));


        //3、转换为POJO，先分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map( line ->{
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 4、分组开窗聚合，得到每个窗口内每个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter( data -> "pv".equals(data.getBehavior()))
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());


        // 5、收集同一窗口的所有商品count数据，排序输出top n
        DataStream<String> resultStream = windowAggStream //按照窗口分组
                .keyBy("WindowEnd")   // 按照窗口分组
                        .process(new TopNHotItems(5)); //用自定义处理函数排序前5

        resultStream.print();

        environment.execute("hot items analysis");
    }

    // 实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator +1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    // 实现自定义的KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String>{
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount,同一key共享，即同一窗口数据共享list
        ListState<ItemViewCount> itemCountAggListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemCountAggListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, KeyedProcessFunction<Tuple, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 每当来一条数据就记录到list，并注册定时器
            itemCountAggListState.add(value);
            // 定时器是按照时间戳来区分
            ctx.timerService().registerProcessingTimeTimer( value.getWindowEnd() + 1 );
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集所有数据，排序
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemCountAggListState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return Integer.compare(o2.getCount().intValue(), o1.getCount().intValue());
                }
            });

            // 将排名信息格式化为String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("=============================").append(System.lineSeparator());
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append(System.lineSeparator());
            }

            resultBuilder.append("===============================").append(System.lineSeparator());

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
