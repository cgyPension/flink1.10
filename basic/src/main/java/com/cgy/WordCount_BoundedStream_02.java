package com.cgy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.FlatMapIterator;
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
// 有界流就是有结束边界的流，比如文件
public class WordCount_BoundedStream_02 {

    public static void main(String[] args) throws Exception {

        // 1 创建流式执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 读取文件
        DataStreamSource<String> line = senv.readTextFile("basic/input");

        // 3 转换数据格式

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = line.flatMap(
                (value, out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                        // 使用采集器，往下游发送数据
                        out.collect(tuple2);
                    }
                }
        );

        // 扩展  TODO 仅了解：
        // 使用lambda表达式，flink不认识Tuple，需要用returns明确指定
        // Flink内部对类型封装成 TypeInformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne1 = wordAndOne.returns(new TypeHint<Tuple2<String, Integer>>() {
        });

        // 4 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneGS = wordAndOne1.keyBy(0);


        // 5 求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = wordAndOneGS.sum(1);

        // 6 打印结果
        wordCount.print();

        // 7 执行 流式环境 在代码的最后，需要调用执行方法，否则流处理逻辑不会执行
        senv.execute();
    }

}


/**
 * Description:
 */

