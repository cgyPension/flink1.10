package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 *
 */

public class Flink10_TimeCharacteristic_Watermark_Ascending {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        // TODO 指定为 事件时间 语义
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<WaterSensor> sensorDS = senv
//                .readTextFile("input/sensor-data.log")
                .socketTextStream("hadoop102",9999 )
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                //TODO 升序场景下的 事件时间提取 和 watermark生成
                // Ascending是官方提供的一个抽象类，适用于 数据的 事件时间 是 单调递增 的情况
                // new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
                // 升序场景的 watermark = EventTime - 1ms
                .assignTimestampsAndWatermarks(
                        // 升序
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        /**
         * TODO 窗口的划分
         * TumblingEventTimeWindows.assignWindows()
         *      => 窗口的开始时间 = timestamp - (timestamp - offset + windowSize) % windowSize;
         *                  1549044122 - (1549044122 + 5) % 5 = 1549044120  => 向下取整（整：窗口长度的整数倍）
         *      => 窗口的结束时间 = start + size => 窗口开始时间 + 窗口长度
         * TimeWindow.maxTimestamp()
         *      => 窗口的数据范围： 左闭右开 => maxTimestamp = end - 1ms;
         *
         * TODO 窗口什么时候触发
         * EventTimeTrigger.onElement()
         *  => window.maxTimestamp() <= ctx.getCurrentWatermark() 触发计算
         */

        sensorDS.keyBy(sensor->sensor.getId())
                .timeWindow(Time.seconds(5))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                System.out.println("process...");
                                out.collect("当前key=" + s + ",一共有" + elements.spliterator().estimateSize() + "条数据");
                            }
                        }
                ).print();

        senv.execute();

    }

}



