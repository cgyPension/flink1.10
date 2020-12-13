package com.cgy.flink_Sql;

import com.cgy.bean.WaterSensor;
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

import javax.xml.crypto.Data;

/**
 * @author GyuanYuan Cai
 * 2020/11/7
 * Description:
 */

public class Flink03_TableAPI_API {
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
        // 1.创建 表的执行环境,不管是 TableAPI还是SQL，都需要先做这一步
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2 把DataStream 转成一个Table
        Table table = tableEnv.fromDataStream(sensorDS, "id,vc,ts as times");

        // 3 对table 进行操作
        Table resultTable = table.where("id = 'sensor_1'")
                .select("id,times,vc");

        // 4 定义一个 外部系统的抽象 Table
        // TODO 把 外部系统 抽象成 一个 Table对象，那么就变成了 Table 与 Table之间的操作
        //  通过connect与外部系统产生关联
        //  withFormar指定数据在 外部系统里的存储格式
        //  Schema指定 抽象成的 Table的 表结构信息 => 字段名和类型
        //  createTemporaryTable 给 抽象Table 起一个 表名
        //  => 可以作为Sink，也可以作为Source
         tableEnv.connect(new FileSystem().path("basic/out/flink.txt"))
                 .withFormat(new OldCsv().fieldDelimiter("|"))
                 .withSchema(
                         new Schema()
                         .field("aaa", DataTypes.STRING())
                         .field("bbb",DataTypes.BIGINT())
                         .field("ccc",DataTypes.INT())
                 )
                 .createTemporaryTable("hahaha");

        // 5 保存数据
        resultTable.insertInto("hahaha");

        env.execute();
    }

}