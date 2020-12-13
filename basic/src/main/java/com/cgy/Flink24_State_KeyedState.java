package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * @author GyuanYuan Cai
 * 2020/10/31
 * Description:
 */

public class Flink24_State_KeyedState {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
                .assignTimestampsAndWatermarks(
                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {

                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                return element.getTs() * 1000L;
                            }

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                                return new Watermark(extractedTimestamp);
                            }
                        }
                );


        SingleOutputStreamOperator<String> resultDS = sensorDS.keyBy(sensor -> sensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    // 1 定义状态
                    ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 2. 在open中初始化状态
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("当前key=" + ctx.getCurrentKey() + ",值状态保存的上一次水位值=" + valueState.value());
                        valueState.update(value.getVc());
                    }
                });

        resultDS.print();

        senv.execute();
    }

}