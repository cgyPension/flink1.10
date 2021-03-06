package com.cgy.flink_Sql;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
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

public class Flink07_SQL_API {
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

        SingleOutputStreamOperator<WaterSensor> sensorDS1 = env
                .readTextFile("basic/input/sensor-data-cep.log")
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

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //  把 DataStream 转成一个 Table
        tableEnv.createTemporaryView("sensorTable",sensorDS,"id,vc,ts");
        tableEnv.createTemporaryView("sensorTable1",sensorDS1,"id,vc,ts");

        // TODO 使用 SQL 对 Table进行操作
        Table resultTasble = tableEnv.sqlQuery("select id,vc,ts from sensorTable where id = 'sensor_1'");
//                .sqlQuery("select " +
//                        "id,count(id) cnt " +
//                        "from sensorTable " +
//                        "group by id");
//                .sqlQuery(
//                        "select " +
//                                "* " +
//                                "from sensorTable s1 " +
//                                "right join sensorTable1 s2 " +
//                                "on s1.id=s2.id");
//                .sqlQuery(
//
//                        "select * from sensorTable " +
//                                "union all " +
//                                "select * from sensorTable1");
//                .sqlQuery(
//                        "select * from sensorTable where id not in (select id from sensorTable1)");

        tableEnv.toRetractStream(resultTasble,Row.class).print();

        env.execute();
    }

}