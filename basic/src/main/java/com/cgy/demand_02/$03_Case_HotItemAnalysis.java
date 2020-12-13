package com.cgy.demand_02;

/**
 * @author GyuanYuan Cai
 * 2020/11/3
 * Description:
 */

import com.cgy.bean.ItemCountWithWindowEnd;
import com.cgy.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

/**
 *  数据：
 *
 *  需求：每隔5分钟输出最近一小时内点击量最多的前N个商品
 *
 * 	1、创建执行环境
 * 	2、准备数据 => 读取、转换
 * 	3、处理逻辑
 * 		=> 过滤？ => 要过滤，只要 pv 行为
 * 		=> 怎么分组？ => 按照统计维度分组 => 商品
 * 		=> 根据需求开窗 => 滑动窗口，长度 1小时，步长 5分钟
 * 		=> 求和、排序、取前N个？？？？
 * 	    => 怎么求和？ => 聚合完之后， TODO 窗口信息就没了 => 用 aggregate 传两个参数
 * 				==> 第一个参数 ；预聚合，结果传递给 全窗口函数
 * 				==> 第二个参数 ：全窗口函数， 给 统计结果 打上 窗口标记（窗口结束时间）
 * 			=> 按照 窗口结束时间 分组 => 让 属于 同一个窗口的 统计结果 汇聚到一起
 * 		=> 排序
 * 			=> process => process 是来一条处理一条，所以 定义一个 ListState 先把数据存起来
 * 			=> 存到什么时候 => 模拟窗口的触发， 使用定时器 => 注册的时间，窗口的结束时间（可以考虑延迟一点，加个100ms）
 * 			=> 定时器触发，说明数据到齐了
 * 			=> 排序方式： 1. 把数据放到一个List里，调用List的sort方法 => 实现 Comparator 接口，重写compare方法 => 前减后 升序 => 后减前 降序
 * 		=> 取前N
 * 			怎么取？ 就是list的前N个，索引从 0~N-1 取出就行
 * 	4、输出：打印
 * 		=> 可以sink到 mysql、redis、es、kafka
 *
 * 	5、执行 env.execute();
 *
 */
public class $03_Case_HotItemAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<UserBehavior> userbehaviorDS = senv.readTextFile("basic/input/UserBehavior.csv").map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] datas = value.split(",");
                return new UserBehavior(
                        Long.valueOf(datas[0]),
                        Long.valueOf(datas[1]),
                        Integer.valueOf(datas[2]),
                        datas[3],
                        Long.valueOf(datas[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getItemId() * 1000L;
            }
        });

        SingleOutputStreamOperator<UserBehavior> filterDS = userbehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        KeyedStream<UserBehavior, Long> userBehaviorKS = filterDS.keyBy(data -> data.getItemId());

        WindowedStream<UserBehavior, Long, TimeWindow> userBehaviorWS = userBehaviorKS.timeWindow(Time.hours(1), Time.minutes(5));
        // TODO 窗口内 求和、排序、取前N个
        // 2.4 求和?
        // sum ? => 转成 （商品ID，1） => 得到 （1001，10），（1002，20） 调用之后就不是窗口流了，窗口会关闭
//          // reduce？ => 可以得到统计结果,输入和输出的类型要一致 => 不能排序，没法隔离窗口
//        // aggregate => 可以得到统计结果 => 而且是增量递增、输入输出类型可以不一样
//        // process => 可以得到统计结果 => 是全窗口函数，会存数据，有oom风险

        // aggregate传两个参数：
        //     => 第一个参数：AggregateFunction ，增量的聚合函数， 输出 给 第二个参数作为输入
        //     => 第二个参数：全窗口函数， 它的输入就是 与聚合函数的 输出
        // 假设有如下商品
        //  1001
        //  1001
        //  1001
        //  1001
        //  1002
        //  1002
        // => 预聚合函数，得到 4、2 两条结果数据
        // => 预聚合函数的结果 输出给  全窗口函数 （不需要在数据中指明分组的key(商品ID)，这个key在上下文可以获取）
        // => 中小电商，商品大概有 几万 到 小几十万， 所以 预聚合的结果，大概也只有 几万到小几十万，而且每一条都只是一个 统计的数字
        SingleOutputStreamOperator<ItemCountWithWindowEnd> aggWithWidowEndDS = userBehaviorWS.aggregate(new AggFunction(), new CountByWindowEnd());

        // 窗口结束时间 分组 => 为了让 同一个窗口的统计结果 放到同一个分组，方便后续进行排序
        KeyedStream<ItemCountWithWindowEnd, Long> aggKS = aggWithWidowEndDS.keyBy(data -> data.getWindowEnd());

        // 使用 process 进行排序
        aggKS.process(new TopNProcessFunction(3))
                .print();
        senv.execute();
    }

    // 预聚合函数：他的输出 是 全窗口函数的 输入
    private static class AggFunction implements AggregateFunction<UserBehavior,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    /**
     * 全窗口函数：把预聚合的结果，打上 窗口结束时间 的标签
     * => 输入类型 就是 预聚合的结果，打上 窗口结束时间 的标签
     * => 为了后续 按照窗口结束时间分组，让同一个窗口的统计结果在一起，进行排序
     */
    private static class CountByWindowEnd extends ProcessWindowFunction<Long,ItemCountWithWindowEnd,Long,TimeWindow> {

        /**
         * 进入这个方法，是 同一个窗口的 同一个分组的 全部数据
         * => 对于 1001 这个商品（分组）来说，已经是一个预聚合的结果， 只有一条 4
         * @param itemId
         * @param context
         * @param elements
         * @param out
         * @throws Exception
         */
        @Override
        public void process(Long itemId, Context context, Iterable<Long> elements, Collector<ItemCountWithWindowEnd> out) throws Exception {
            out.collect(new ItemCountWithWindowEnd(itemId,elements.iterator().next(),context.window().getEnd()));// elements.iterator().next() 累加器里获取
        }
    }

    private static class TopNProcessFunction extends KeyedProcessFunction<Long,ItemCountWithWindowEnd,String> {

        ListState<ItemCountWithWindowEnd> datas;
        ValueState<Long> triggerTS;

        private int threshold;
        private int currentThreshold;

        public TopNProcessFunction(int threshold){
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            datas =getRuntimeContext().getListState(new ListStateDescriptor<ItemCountWithWindowEnd>("datas",ItemCountWithWindowEnd.class));
            triggerTS = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTs",Long.class));
        }

        @Override
        public void processElement(ItemCountWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
            // 排序
            // 数据是一条一条处理的，所以先把数据存起来
            datas.add(value);
            // 存到啥时候？ => 等本窗口的所有数据到齐 => 模拟窗口的触发 => 定时器 =》 预留点时间
            if(triggerTS.value() == null){
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+10L); // 注册的是执行时间 定时的时间 <= watermark 才会触发定时器
                triggerTS.update(value.getWindowEnd()+10L);
            }
        }

        // 定时器触发：说明 同一窗口的 统计结果 已经到齐了，进行排序、取前N个
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterable<ItemCountWithWindowEnd> datasIt = datas.get();
            // 创建一个List，用来 存 数据，进行排序
            ArrayList<ItemCountWithWindowEnd> itemCountWithWindowEnds = new ArrayList<>();
            for (ItemCountWithWindowEnd itemCountWithWindowEnd : datasIt) {
                itemCountWithWindowEnds.add(itemCountWithWindowEnd);
            }
            // 清空保存的 数据和时间
            datas.clear();
            triggerTS.clear();

            // 排序
 /*           itemCountWithWindowEnds.sort(
                    new Comparator<ItemCountWithWindowEnd>() {
                        @Override
                        public int compare(ItemCountWithWindowEnd o1, ItemCountWithWindowEnd o2) {
                             // 后 减 前 -> 降序，这里要降序；
                            // 前 减 后 -> 升序
                            return o2.getItemCount().intValue()-o1.getItemCount().intValue();
                        }
                    }
            );*/

            // java 集合 自带的集合排序
            Collections.sort(itemCountWithWindowEnds);

            // 取前N个
            StringBuffer resultStr = new StringBuffer(); // 线程安全的
            resultStr.append("---------------------------------------\n");
            resultStr.append("窗口结束时间："+(timestamp-10L)+"\n");

            // 判断一下 传参 与实际个数的 大小，防止 越界问题
            currentThreshold=threshold>itemCountWithWindowEnds.size()?itemCountWithWindowEnds.size():threshold;
            for (int i = 0; i < currentThreshold; i++) {
                 resultStr.append("Top"+(i+1)+":"+ itemCountWithWindowEnds.get(i)+"\n");
            }
            resultStr.append("-------------------------------------------------------\n\n\n");
                    out.collect(resultStr.toString());
        }
    }
}