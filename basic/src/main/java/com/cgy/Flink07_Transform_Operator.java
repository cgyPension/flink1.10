package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

import javax.swing.text.ParagraphView;

/**
 * @author GyuanYuan Cai
 * 2020/10/28
 * Description:
 */

// TODO 滚动聚合算子(keyby之后)（Rolling Aggregation）
public class Flink07_Transform_Operator {

    StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void transfrom_Keyby_m() throws Exception {
        senv.setParallelism(1);
        //DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");
        DataStreamSource<String> fileDS = senv.socketTextStream("hadoop102",9999);

        SingleOutputStreamOperator<WaterSensor> sensorDS = fileDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] datas = s.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
            }
        });

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());
        // 默认只更新聚合的字段，其他字段不更新
        sensorKS.sum("vc").print("sum");
        sensorKS.max("vc").print("max");
        sensorKS.min("vc").print("min");
        senv.execute();

    }

    @Test
    public void transfrom_reduce() throws Exception {
        senv.setParallelism(1);
        //DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");
        DataStreamSource<String> fileDS = senv.socketTextStream("hadoop102",9999);

        SingleOutputStreamOperator<WaterSensor> sensorDS = fileDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] datas = s.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
            }
        });

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // TODO reduce
        // 输入类型 和 输出类型必须一致
        // 同一个分组的 第一条数据 不会进入reduce方法
        // reduce会帮我们保存之前的聚合结果，也就是状态
        sensorKS.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
                System.out.println(waterSensor + "<------>" + t1);
                return new WaterSensor(waterSensor.getId(), System.currentTimeMillis(), waterSensor.getVc() + t1.getVc());
            }
        }).print();
        senv.execute();
    }

    @Test
    public void transfrom_process() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");
        //DataStreamSource<String> fileDS = senv.socketTextStream("hadoop102",9999);

        SingleOutputStreamOperator<WaterSensor> sensorDS = fileDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] datas = s.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
            }
        });

        // Flink在数据流通过keyBy进行分流处理后，如果想要处理过程中获取环境相关信息，可以采用process算子自定义实现

         sensorDS.keyBy(sensor -> sensor.getId()).process(new KeyedProcessFunction<String, WaterSensor, String>() {
             @Override
             public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                 out.collect(value.toString()+"---"+ctx.getCurrentKey());
             }
         }).print();

        senv.execute();
    }


}


/**
 * Description:
 */

