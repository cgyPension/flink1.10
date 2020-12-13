package com.cgy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author GyuanYuan Cai
 * 2020/10/26
 * Description:
 */

// 流处理WordCount
// 所谓的无界流就是没有结束边界的流，如网络，消息队列
public class WordCount_UnBoundedStream_03 {
    public static void main(String[] args) throws Exception {

        // 1 创建流式执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 从socket读取
       // DataStreamSource<String> line = senv.socketTextStream("localhost", 9999);
        DataStreamSource<String> line = senv.socketTextStream("hadoop102", 9999);

        // 3 转换数据格式：压平、切分、转换成元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = line.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        // 4 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneGS = wordAndOne.keyBy(0);

        // 5 求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = wordAndOneGS.sum(1);

        // 6 打印结果
        wordCount.print();

        // 7 执行
        senv.execute();
    }

}


/**
 * Description:
 */

