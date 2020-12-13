package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * @author GyuanYuan Cai
 * 2020/10/30
 * Description:
 */

public class Flink13_Watermark_Punctuated {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        // TODO 指定为 事件时间 语义
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //TODO 设置 周期性 生成 Watermark的时间间隔，默认200ms，一般不改动
//        senv.getConfig().setAutoWatermarkInterval(5000L);

        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = senv
//                .readTextFile("input/sensor-data.log")
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                //TODO Punctuated - watermark
                .assignTimestampsAndWatermarks(
                        //TODO Punctuated方式：间歇性的 => 来一条数据，生成一次 watermark
                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {

                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                System.out.println("extractTimestamp...");
                                return element.getTs() * 1000L;
                            }

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                                System.out.println("checkAndGetNextWatermark...");
                                return new Watermark(extractedTimestamp);
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
                            /**
                             * 输入的数据是：整个窗口 同一分组 的数据 一起 处理
                             * @param s
                             * @param context
                             * @param elements
                             * @param out
                             * @throws Exception
                             */
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

        senv.execute();
    }


}


/**
 * Description:
 */

