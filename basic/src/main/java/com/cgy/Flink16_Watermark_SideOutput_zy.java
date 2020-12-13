package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 样例类：水位传感器：用于接收空高数据
 * id:传感器编号
 * ts:时间戳
 * vc:空高
 * String id, Long ts, Integer vc
 */

public class Flink16_Watermark_SideOutput_zy {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 1.env指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                })
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(3)){
                            @Override
                            public long extractTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        // 2.处理数据
        // TODO 窗口关闭后 的 迟到数据 处理 --- 侧输出流
        // 1.在窗口真正关闭后，还有迟到数据，那么就放入到 侧输出流中


        // TODO 创建 OutputTag实例的时候，必须指定泛型，必须给花括号
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("late data"){};

        SingleOutputStreamOperator<Long> resultDS = sensorDS
                .keyBy(data -> data.getId())
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
                .process(
                        /**
                         * 全窗口函数：整个窗口的本组数据，存起来，关窗的时候一次性一起计算
                         */
                        new ProcessWindowFunction<WaterSensor, Long, String, TimeWindow>() {

                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<Long> out) throws Exception {
                                out.collect(elements.spliterator().estimateSize());
                            }
                        });

        // TODO 获取 侧输出流
        // 获取到的是一个 DataStream，可以自行跟 主流 做一个关联(connect、union)，汇总结果
        resultDS.getSideOutput(outputTag).print("late data in side-output");
        resultDS.print("result");

        env.execute();
    }

}

