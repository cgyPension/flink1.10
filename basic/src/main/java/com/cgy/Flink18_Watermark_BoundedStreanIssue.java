package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * TODO Watermark总结
 * 1.概念和理解
 *  => 解决乱序的问题
 *  => 表示 事件时间 的进展
 *  => 是一个特殊的时间戳 (插入到数据流里，随着流而往下游传递)
 *  => 单调递增的
 *  => 认为 在它 之前的数据已经处理过了
 *
 * 2.官方提供的实现
 *  => 升序 watermark = EventTime - 1ms
 *  => 乱序 watermark = EventTime - 最大乱序程度（自己设的）
 *
 * 3.自定义生成 watermark
 *  => 周期性，默认周期 200ms，可以修改
 *  => 间歇性，来一条，生成一次 watermark
 *
 * 4.watermark的传递、多并行度的影响
 *  以 多个 并行子任务 中，watermark最小的为准 => 参考 木桶原理，因为最小的watermark之前肯定处理了
 *
 * 5.有界流的问题
 *  有界流在关闭之前，会把 watermark设为 Long的最大值 => 目的是为了保证所有的窗口都被触发、所有的数据都被计算
 *
 *
 * TODO Flink对乱序和迟到数据的处理、保证
 * 1.watermark设置 乱序程度（等待时间） => 解决乱序
 * 2.窗口等待时间 => 窗口再等待一会，处理关窗前迟到数据
 * 3.侧输出流 => 关窗后的迟到数据,放入到 侧输出流中，可以自行跟 主流 做一个关联，汇总结果
 */
public class Flink18_Watermark_BoundedStreanIssue {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // TODO 有界流 关于 watermark的问题
        // 为了保证所有的数据，都能被计算，所以，会在结束之前，把 watermark设置为 Long的最大值
        // 因为升序的watermark是周期性的200ms，读取完数据，还没达到200ms，也就是watermark还来不及更新，就处理完毕
        //      => 所以看起来，所有窗口同时被 Long的最大值 触发


        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("basic/input/sensor-data.log")
//                .socketTextStream("localhost",9999 )
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {
                            private Long maxTs = Long.MIN_VALUE;

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                                maxTs = Math.max(maxTs, extractedTimestamp);
                                return new Watermark(maxTs);
                            }

                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }
                );
        // 2.处理数据
        // 分组、开窗、全窗口函数
        sensorDS
                .keyBy(sensor -> sensor.getId())
                .timeWindow(Time.seconds(5))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                System.out.println("process...");
                                out.collect("当前key=" + s
                                        + "当前watermark=" + context.currentWatermark()
                                        + "窗口为[" + context.window().getStart() + "," + context.window().getEnd() + ")"
                                        + ",一共有" + elements.spliterator().estimateSize() + "条数据");
                            }
                        }
                )
                .print();

        env.execute();
    }
}
