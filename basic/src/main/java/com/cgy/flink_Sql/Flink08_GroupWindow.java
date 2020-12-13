package com.cgy.flink_Sql;

import com.cgy.bean.WaterSensor;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author GyuanYuan Cai
 * 2020/11/9
 * Description:
 */

public class Flink08_GroupWindow {
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

        tableEnv.createTemporaryView("sensorTable",sensorDS,"id,vc,ts.rowtime as rt");
//        tableEnv.createTemporaryView("sensorTable",sensorDS,"id,vc,ts.proctime as rt");
        Table sensorTable = tableEnv.from("sensorTable");

        /**
         * TODO 使用 TableAPI 实现 GroupWindow
         * 1  用window来指定窗口
         *      => 首先 指定窗口类型，Tumble、Slide、Session
         *      => 然后 指定窗口参数，over(“时间（行数）.时间单位复数（或rows）”)
         *      => 接着 指定 用来分组（按时间间隔）或者排序（按行数）的时间字段
         *      => 字段的指定，用 时间字段.rowtime 或时间字段.proctime，可以起别名
         *      => 最后 指定窗口的别名，as（“别名”）
         * 2  将 窗口 放在分组字段里，也就是放在 groupby 里
          */
/*        Table resultTable = sensorTable
                .window(Tumble.over("5.seconds").on("rt").as("w"))
                .groupBy("id,w")
                .select("id,count(id) as cnt,w.start,w.end");*/

        Table resultTable = tableEnv.sqlQuery("select " +
                "id," +
                "count(id) as cnt," +
                "HOP_START(rt,interval '2' second,interval '5' second) as window_start," +
                "HOP_END(rt,interval '2' second,interval '5' second) as window_end " +
                "from sensorTable " +
                "group by id,HOP(rt,interval '2' second,interval '5' second)");

        tableEnv.toRetractStream(resultTable,Row.class).print();
        // TODO 使用 SQL 实现 GroupWindow

        env.execute();
    }

}