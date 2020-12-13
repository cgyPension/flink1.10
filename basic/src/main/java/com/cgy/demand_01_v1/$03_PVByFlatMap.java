package com.cgy.demand_01_v1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author GyuanYuan Cai
 * 2020/10/28
 * Description:
 */

// 需求一： 网站总浏览量（PV）的统计
public class $03_PVByFlatMap {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        // 2 读取数据
        senv.readTextFile("basic/input/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String datas = value.split(",")[3];
                        if ("pv".equals(datas)) {
                            out.collect(Tuple2.of("pv", 1));
                        }
                    }
                })
                .keyBy(data -> data.f0)
                .sum(1)
                .print();


        // 4 执行
        senv.execute();
    }

}


/**
 * Description:
 */

