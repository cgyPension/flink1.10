package com.cgy.flink_Sql;

import com.cgy.bean.WaterSensor;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author GyuanYuan Cai
 * 2020/11/7
 * Description:
 */

public class Flink05_SQL_API {
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

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Table table = tableEnv.fromDataStream(sensorDS, "id,vc,ts as times");
         tableEnv.createTemporaryView("sensorTable",table);
//         tableEnv.createTemporaryView("sensorTable",sensorDS, "id,vc,ts as times");


        /**
         * TODO 创建 Table  的方式
         *  1  从DataStream转换成Table
         *      => 一种不给名字，fromDataStreamream，后续可以给名字
         *      => 一种直接给名字，tableEnv.创建临时视图
         *  2  从  外部系统  抽象成一个Table
         *      => 指定连接、指定格式、指定结构信息=> 指定表名
         *      => 从 表名 获取 Table对象
          */
        Table resultTable = tableEnv.sqlQuery("select id,vc,times from sensorTable");

        tableEnv.toAppendStream(resultTable, Row.class).print();

        env.execute();
    }

}