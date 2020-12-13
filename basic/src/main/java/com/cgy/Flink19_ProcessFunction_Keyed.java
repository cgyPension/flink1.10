package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author GyuanYuan Cai
 * 2020/10/31
 * Description:
 */

public class Flink19_ProcessFunction_Keyed {

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
                        new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(3)) {
                            @Override
                            public long extractTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        sensorDS
                .keyBy(sensor->sensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        System.out.println(
                            "data="+value+
                            ",当前key="+ctx.getCurrentKey()+
                            ",ts="+ctx.timestamp()+
                            ",wm="+ctx.timerService().currentWatermark()+
                            ",处理时间="+ctx.timerService().currentProcessingTime()
                        );

                        // 获取当前 key
                        System.out.println(ctx.getCurrentKey());
                        // 把数据放入侧输出流 => 第一个参数：标签对象；第二个参数，要放入侧输出流的数据
                        ctx.output(new OutputTag<WaterSensor>("late data"){},value);

                    }
                }).print();

        senv.execute();
    }
}