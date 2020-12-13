package com.cgy.flink_Sql;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
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

/**
 * @author GyuanYuan Cai
 * 2020/11/7
 * Description:
 */

public class Flink02_TableAPI_API {
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
        // 1 创建 表的执行环境,不管是 TableAPI还是SQL，都需要先做这一步
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();

        // 2 把 DataStream 转成一个 Table
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Table table = tableEnv.fromDataStream(sensorDS, "id,vc,ts as times");

        // 3 对 Table进行操作
        Table resultTable = table
                .groupBy("id")
//                .aggregate("count(id) as cnt")
                .select("id,count(id) as cnt");

        // 4 把处理完的 Table 转成 DataStream
        // toRetractStream：撤回流，涉及数据的修改更新，必须用撤回流
        // 实现方式：把原来的数据，标记为false,表示撤回
        //         把更新后的数据，标记为true，表示添加
        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnv.toRetractStream(resultTable, Row.class);

        resultDS.print();

        env.execute();
    }

}