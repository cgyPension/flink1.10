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
 * 2020/11/9
 * Description:
 */

public class Flink09_OverWindow {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
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


        // 1.创建 表的执行环境,不管是 TableAPI还是SQL，都需要先做这一步
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 把 DataStream 转成一个 Table
        tableEnv.createTemporaryView("sensorTable", sensorDS, "id,vc,ts.rowtime as rt");
//        tableEnv.createTemporaryView("sensorTable", sensorDS, "id,vc,ts.proctime as rt");
        Table sensorTable = tableEnv.from("sensorTable");


        // TODO 使用 TableAPI实现 OverWindow
/*        Table resultTable = sensorTable
                .window(
                        Over
                                .partitionBy("id")
                                .orderBy("rt")
                                .preceding("UNBOUNDED_RANGE")
                                .following("CURRENT_RANGE")
                                .as("ow"))
                .select("id,vc,rt,count(id) over ow");*/



        // TODO 使用 SQL 实现 OverWindow
        Table resultTable = tableEnv.sqlQuery("select " +
                "id,vc,rt," +
                "count(id) over(partition by id order by rt) as cnt " +
                "from sensorTable");


        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute();
    }

}