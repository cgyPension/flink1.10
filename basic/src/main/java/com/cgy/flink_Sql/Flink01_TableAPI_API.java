package com.cgy.flink_Sql;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.stream.Stream;

/**
 * @author GyuanYuan Cai
 * 2020/11/7
 * Description:
 */

public class Flink01_TableAPI_API {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("basic/input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        // TODO TableAPI基本使用
        // 1 创建 表的执行环境，不管是 TableAPI还是SQL，都需要先做这一步
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2 把 DataStream 转成一个 Table
        // 字段一样 会自动识别到样例类里的顺序
        Table table = tableEnv.fromDataStream(sensorDS, "id,vc,ts as times");

        // 3 对 Table进行操作
        Table resultTable = table.where(" id = 'sensor_1'")
//                .filter("id = 'sensor_1'")
                .select("id,times");

        // 4 把处理完的 Table 转换 DataStream
//        DataStream<Row> resultDS = tableEnv.toAppendStream(resultTable, Row.class);// 追加流
//        DataStream<Tuple2<String, Long>> resultDS = tableEnv.toAppendStream(resultTable, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
//        }));
        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnv.toRetractStream(resultTable, Row.class);

        resultDS.print();

        env.execute();
    }

}