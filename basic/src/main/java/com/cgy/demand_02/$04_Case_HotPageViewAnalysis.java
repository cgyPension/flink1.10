package com.cgy.demand_02;

/**
 * @author GyuanYuan Cai
 * 2020/11/3
 * Description:
 */

import com.cgy.bean.ApacheLog;
import com.cgy.bean.PageCountWithWindow;
import com.cgy.bean.SimpleAggFunction;
import com.typesafe.sslconfig.ssl.FakeChainedKeyStore;
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
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 实时热门页面浏览排名：每隔5秒，输出最近10分钟访问量最多的前N个URL
 *
 * 1 读取数据
 *     切分 封装样例类
 *     watermark 乱序 1分钟
 *     过滤只剩URl的数据
 *
 * 2 业务逻辑
 *     按照url分组 滑动开窗（10min,5s）
 *     增量聚合 来一条加一条 （url，1）
 *     延迟10毫秒 执行定时器回调函数
 *     根据窗口结束时间分组 排序 取TopN排序
 *     打印
 * 3 执行
 */
public class $04_Case_HotPageViewAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<ApacheLog> logDS = senv.readTextFile("basic/input/apache.log").map(new MapFunction<String, ApacheLog>() {
            @Override
            public ApacheLog map(String value) throws Exception {
                String[] datas = value.split(" ");
                return new ApacheLog(
                        datas[0],
                        datas[1],
                        datas[3],
                        datas[5],
                        datas[6]
                );
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(ApacheLog element) {
                return element.getEventTime();
            }
        });

        KeyedStream<ApacheLog, String> logKS = logDS.keyBy(data -> data.getUrl());
        WindowedStream<ApacheLog, String, TimeWindow> logWS = logKS.timeWindow(Time.minutes(10), Time.seconds(5));

        SingleOutputStreamOperator<PageCountWithWindow> aggDS = logWS.aggregate(new SimpleAggFunction<ApacheLog>(),
                new ProcessWindowFunction<Long, PageCountWithWindow, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Long> elements, Collector<PageCountWithWindow> out) throws Exception {
                        out.collect(new PageCountWithWindow(s, elements.iterator().next(), context.window().getEnd()));
                    }
                }
        );

        KeyedStream<PageCountWithWindow, Long> aggKS = aggDS.keyBy(data -> data.getWindowEnd());

        aggKS.process(new TopNPageView(5)).print();

        senv.execute();
    }

    private static class TopNPageView extends KeyedProcessFunction<Long,PageCountWithWindow,String> {

        ListState<PageCountWithWindow> dataList;
        ValueState<Long> timerTs;

        private int threshold;

        public TopNPageView(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            dataList = getRuntimeContext().getListState(new ListStateDescriptor<PageCountWithWindow>("dataList",PageCountWithWindow.class));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs",Long.class,0L));
        }

        @Override
        public void processElement(PageCountWithWindow value, Context ctx, Collector<String> out) throws Exception {
            dataList.add(value);
            // 数据到齐了，开始排序 => 模拟窗口的触发 => 定时器，给个延迟
            if (timerTs.value() == 0) {
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1L);
                timerTs.update(value.getWindowEnd()+1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 从状态 取出 数据
            ArrayList<PageCountWithWindow> datas = new ArrayList<>();
            for (PageCountWithWindow pageCountWithWindow : dataList.get()) {
                datas.add(pageCountWithWindow);
            }
            // 清空状态
            dataList.clear();
            timerTs.clear();

            // 排序
            datas.sort(new Comparator<PageCountWithWindow>() {
                @Override
                public int compare(PageCountWithWindow o1, PageCountWithWindow o2) {
                    long result = o2.getPageCount() - o1.getPageCount();
                    if (result<0) {
                        return  -1;
                    } else if (result > 0) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            // 取前 N
            StringBuffer resultStr = new StringBuffer();
            Timestamp windowEndDate = new Timestamp(timestamp - 1);
            resultStr.append("窗口结束时间："+ windowEndDate+"\n");

            for (int i = 0; i < (threshold>datas.size()?datas.size():threshold); i++) {
                 resultStr.append("Top"+(i+1)+":"+datas.get(i)+"\n");
            }
            resultStr.append("===============================================");
                    out.collect(resultStr.toString());
        }
    }
}