package com.cgy.demand_01_v1;

/**
 * @author GyuanYuan Cai
 * 2020/10/30
 * Description:
 */

import com.cgy.bean.AdClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  数据格式：
 *  用户ID、广告ID、省份、城市、时间戳   以逗号分隔
 *  Long userId, Long adId, String province, String city, Long timestamp
 *
 *  页面广告点击量实时统计
 *  按照 统计维度 （省份、广告） 分组
 */

public class $07_AdClickAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        SingleOutputStreamOperator<AdClickLog> adClickDS = senv.readTextFile("basic/input/AdClickLog.csv")
                .map(new MapFunction<String, AdClickLog>() {
                    @Override
                    public AdClickLog map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new AdClickLog(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                datas[2],
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                });

       adClickDS.map(new MapFunction<AdClickLog, Tuple2<String,Integer>>() {
           @Override
           public Tuple2<String, Integer> map(AdClickLog value) throws Exception {
               return Tuple2.of(value.getProvince()+"_"+value.getAdId(),1);
           }
       })
        .keyBy(data->data.f0)
        .sum(1)
        .print();

       senv.execute();

    }

}


/**
 * Description:
 */

