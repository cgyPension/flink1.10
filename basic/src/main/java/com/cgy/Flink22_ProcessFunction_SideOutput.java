package com.cgy;

import akka.protobuf.ByteString;
import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.omg.CORBA.portable.ValueOutputStream;

import javax.annotation.Nullable;
import javax.xml.crypto.Data;

/**
 * @author GyuanYuan Cai
 * 2020/10/31
 * Description:
 */

// 水位高于 50 的放入侧输出流，其他正常在 主流里
public class Flink22_ProcessFunction_SideOutput {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = senv
                .readTextFile("basic/input/sensor-data.log")
                //.socketTextStream("localhost", 9999)
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

        //TODO 使用侧输出流
        // 1.定义一个OutputTag，给定一个 名称
        // 2.使用 ctx.output(outputTag对象,放入侧输出流的数据)
        // 3.获取侧输出流 => DataStream.getSideOutput(outputTag对象)
        OutputTag<String> higTag = new OutputTag<String>("high-level"){};

        SingleOutputStreamOperator<WaterSensor> resultDS = sensorDS.keyBy(sensor -> sensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        if (value.getVc() > 50) {
                            // TODO 侧输出流
                            // 1 类型可以和主流不一致
                            // 2 通常可以用来：在不影响 主流 逻辑的 情况下，过滤、发现异常数据
                            ctx.output(higTag, "水位值高于50米！！！快跑！！");
                        } else {
                            out.collect(value);
                        }
                    }
                });
        resultDS.print("main-stream");

        DataStream<String> sideOutput = resultDS.getSideOutput(higTag);
        sideOutput.print("side-output");

        senv.execute();
    }

}