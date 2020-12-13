package com.cgy.demand_02;

import com.cgy.bean.AdClickCountWithWindowEnd;
import com.cgy.bean.AdClickLog;
import com.cgy.bean.SimpleAggFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import javax.xml.soap.SOAPEnvelope;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author GyuanYuan Cai
 * 2020/11/3
 * Description:
 */
/**
 *  实时统计 各省份的广告 点击情况
 *
 *  Long userId, Long adId, String province, String city, Long timestamp
 *
 *  1 读取数据
 *      切分 封装样例类 过滤
 *  2 业务逻辑
 *      （省份、广告）分组
 *      timeWindow(Time.hours(1),Time.seconds(5))
 *      窗口后不能用sum聚合
 *      最好用aggregate 增量函数
 *      根据windowEnd分组
 *      用全窗口排序
 *      取TopN的临界值
 *      三元判断
 *      循环写入StringBuffer
 *  3 执行
 */

public class $05_Case_AdClickAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<AdClickLog> adClickDS = senv.readTextFile("basic/input/AdClickLog.csv")
                .map(new MapFunction<String, AdClickLog>() {
                    @Override
                    public AdClickLog map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new AdClickLog(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                datas[2],
                                datas[3],
                                Long.valueOf(datas[4])

                        );
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickLog>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickLog element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 按照 统计维度 （省份、广告） 分组
        adClickDS.keyBy(new KeySelector<AdClickLog, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(AdClickLog value) throws Exception {
                return Tuple2.of(value.getProvince(), value.getAdId());
            }
        }).timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new SimpleAggFunction<>(), new ProcessWindowFunction<Long, AdClickCountWithWindowEnd, Tuple2<String, Long>, TimeWindow>() {
                    @Override
                    public void process(Tuple2<String, Long> stringLongTuple2, Context context, Iterable<Long> elements, Collector<AdClickCountWithWindowEnd> out) throws Exception {
                        out.collect(new AdClickCountWithWindowEnd(stringLongTuple2.f1, stringLongTuple2.f0, elements.iterator().next(), context.window().getEnd()));
                    }
                })
                .keyBy(data -> data.getWindowEnd())
                .process(new TopNAdClick(3))
                .print();

        senv.execute();

    }

    static class TopNAdClick extends KeyedProcessFunction<Long,AdClickCountWithWindowEnd,String> {

        ListState<AdClickCountWithWindowEnd> adClicks;
        ValueState<Long> triggerTs;

        private int threshold;

        public TopNAdClick(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            adClicks = getRuntimeContext().getListState(new ListStateDescriptor<AdClickCountWithWindowEnd>("adClicks",AdClickCountWithWindowEnd.class));
            triggerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTs",Long.class));
        }

        @Override
        public void processElement(AdClickCountWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
            adClicks.add(value);

            if (triggerTs.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1L);
                triggerTs.update(value.getWindowEnd()+1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<AdClickCountWithWindowEnd> datas = new ArrayList<>();
            for (AdClickCountWithWindowEnd adClickCountWithWindowEnd : adClicks.get()) {
                datas.add(adClickCountWithWindowEnd);
            }
            adClicks.clear();
            triggerTs.clear();

            datas.sort(new Comparator<AdClickCountWithWindowEnd>() {
                @Override
                public int compare(AdClickCountWithWindowEnd o1, AdClickCountWithWindowEnd o2) {
                    return o2.getAdClickCount().intValue()- o1.getAdClickCount().intValue();
                }
            });

            StringBuffer resultStr = new StringBuffer();
            resultStr.append("窗口结束时间；"+new Timestamp(timestamp - 1) +"\n");

            for (int i = 0; i < (threshold>datas.size()?datas.size():threshold); i++) {
                resultStr.append("Top"+(i+1)+":"+datas.get(i)+"\n");
            }

            resultStr.append("=================================");
            out.collect(resultStr.toString());

        }
    }
}