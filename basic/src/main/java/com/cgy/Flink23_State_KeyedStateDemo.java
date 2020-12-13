package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * @author GyuanYuan Cai
 * 2020/10/31
 * Description:
 */


public class Flink23_State_KeyedStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        OutputTag<String> highTag  = new OutputTag<String>("high-level") {
        };

        sensorDS.keyBy(sensor->sensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    // ValueState<Object> valueState = getRuntimeContext().getState();


                    // 1 定义状态：
                    ValueState<Integer> valueState;
                    ListState<Long> listState;
                    MapState<String,String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                       // 2 在open中初始化状态
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("list-state",Long.class));
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>("map-state",String.class,String.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        // 3 使用状态
                       /* valueState.value();//取值
                        valueState.update();//更新
                        valueState.clear();//清空

                        listState.add();//添加单个值
                        listState.addAll();//添加一个 list
                        listState.update();//更新整个list
                        listState.clear();//清空

                        mapState.put(,);//添加一个 kv对
                        mapState.putAll();//添加整个map
                        mapState.get();//根据某个key 获取value
                        mapState.remove();//删除指定key的数据
                        mapState.clear();*/
                    }
                });


        senv.execute();
    }

}