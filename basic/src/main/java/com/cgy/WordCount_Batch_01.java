package com.cgy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @author GyuanYuan Cai
 * 2020/10/26
 * Description:
 */

//批处理WordCount

public class WordCount_Batch_01 {
    public static void main(String[] args) throws Exception {

        // 1 创建批式执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2 读取文件 和spark一样是 按照hadoop方式读取
         DataSource<String> line = env.readTextFile("basic/input");

        // 3 转换数据格式
        // 匿名内部类 很多形参都是输入、输出
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = line.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });


        // 4 按照word进行分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGS = wordAndOne.groupBy(0);

        // 5 按照word进行分组
        AggregateOperator<Tuple2<String, Integer>> wordcount = wordAndOneGS.sum(1);

        // 6 打印结果
        wordcount.print();

    }


}




