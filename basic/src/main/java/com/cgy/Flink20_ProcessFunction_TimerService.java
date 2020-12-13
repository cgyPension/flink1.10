package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author GyuanYuan Cai
 * 2020/10/31
 * Description:
 */

// 定时器 很适合做一定时间的定时行为

public class Flink20_ProcessFunction_TimerService {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<WaterSensor> sensorDS = senv.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(
                                datas[0],
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2])
                        );
                    }
                }).assignTimestampsAndWatermarks(
                      /*  new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(3)) {
                            @Override
                            public long extractTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }*/

                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }

                );

        sensorDS
                .keyBy(sensor->sensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private Long triggerTs = 0L;

                    // 来一条数据，处理一条
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp()+5000L);
                        // TODO 创建 & 触发 源码分析
                        //  InternalTimerServiceImpl.registerEventTimeTimer()
                        //	    => 注册 eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
                        //		    =》 为了避免重复注册、重复创建对象，注册定时器的时候，判断一下是否已经注册过了
                        //
                        //  InternalTimerServiceImpl.advanceWatermark()
                        //	    => 触发 timer.getTimestamp() <= time ==========> 定时的时间 <= watermark
                        // 定时的时间 <= watermark 才会触发定时器

                        /* // TODO 定时器
                        // 注册 事件创建时间定时器
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000L);
                        // 注册 处理时间定时器
                        ctx.timerService().registerProcessingTimeTimer(ctx.timestamp());

                        // 注销 事件创建时间定时器
                        ctx.timerService().deleteEventTimeTimer();
                        // 注销 处理时间定时器
                        ctx.timerService().deleteProcessingTimeTimer();*/

                        // 为了避免重复注册、重复创建对象，注册定时器的时候，判断一下是否已经注册过了
                        if (triggerTs == 0) {
                            ctx.timerService().registerEventTimeTimer(
                                    value.getTs()*1000L+5000L
                            );
                            triggerTs = value.getTs()*1000L+5000L;
                        }

                    }

                    // 定时器触发，要执行什么操作
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("onTimer ts="+timestamp+ "定时器触发");
                    }
                }

    ).print();

        senv.execute();

    }
}