package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * @author GyuanYuan Cai
 * 2020/10/27
 * Description:
 */

// 算子
public class Flink06_Transform {


    StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void transfrom_Map() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");

        // TODO map 一进一出
        SingleOutputStreamOperator<WaterSensor> resultDS = fileDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] datas = s.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
            }
        });

        resultDS.print();
        senv.execute();

    }

    @Test
    public void transfrom_MapWithRich() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");

        SingleOutputStreamOperator<WaterSensor> resultDS = fileDS.map(new MyRichMapFunction());


        resultDS.print();
        senv.execute();

    }

    /**
     * 富函数
     * 富有体现在： 1. 有 open和close 生命周期方法 ； 2. 可以获取 运行时上下文 => 获取到一些环境信息、状态....
     *
     * 1. 继承 RichMapFunction，定义两个泛型
     * 2. 实现 map方法 ；  可以定义 open 和close， 可以获取 运行时上下文
     */
    // 并行度为1的情况下，有界流会open一次，close两次（目的是为了确定是不是真的关闭了），大于1的并行度会相乘
    private class MyRichMapFunction extends RichMapFunction<String,WaterSensor> {
        @Override
        public WaterSensor map(String s) throws Exception {
            String[] datas = s.split(",");
            return new WaterSensor(getRuntimeContext().getTaskName(),Long.valueOf(datas[1]),Integer.valueOf(datas[2]));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open......");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close......");
        }
    }



    @Test
    public void transfrom_FlatMap() throws Exception {
        senv.setParallelism(1);
        // 1.读取数据: 有界流 => 有头有尾 => 文件
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");

        // 2.处理数据 TODO flatMap 一进多出
       fileDS.flatMap(
               new FlatMapFunction<String, String>() {
                   @Override
                   public void flatMap(String s, Collector<String> collector) throws Exception {
                       String[] datas = s.split(",");
                       for (String data : datas) {
                           collector.collect(data);
                       }
                   }
               }
       ).print();

        // 3.启动
        senv.execute();

    }

    @Test
    public void transfrom_Filter() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");

        senv.fromCollection(Arrays.asList(1,2,3,4))
 //                .filter(new FilterFunction<Integer>() {
//                    @Override
//                    public boolean filter(Integer value) throws Exception {
//                        return value % 2 == 0;
//                    }
//                })
                .filter(data->data%2==0)  // TODO lomobok表达式 -> 可以想像成Scala的 =>
                .print();
        senv.execute();

    }

    @Test
    public void transfrom_KeyBy() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");

        // 1 读取数据，转成 WaterSensor类型
        SingleOutputStreamOperator<WaterSensor> sensorDS = fileDS.map(file -> {
            String[] datas = file.split(",");
            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        });


        // 2. TODO KeyBy分组
        // 分组，是给 数据打上 分组的标签
        // 分组=》 同一分组 的数据 在一起 => 同一个 slot、subtask 可以有多个分组

        // 根据 位置索引 分组 => 无法推断类型，所以key返回的是 tuple

        // KeyedStream<WaterSensor, Tuple> resultKS = sensorDS.keyBy(0);

        // 根据 属性名称 分组 => 无法推断类型，所以key返回的是 tuple
        // 使用要求：
        // 1、实体类所有变量都是public
        // 2、keyby用到的变量不能是布尔类型的
        // 3、添加无参构造函数

        // KeyedStream<WaterSensor, Tuple> resultKS = sensorDS.keyBy("id");
        // KeyedStream<WaterSensor, String> resultKS = sensorDS.keyBy(new MyKeySelecor());
        KeyedStream<WaterSensor, String> resultKS = sensorDS.keyBy(data -> data.getId());

        // TODO 源码分析
        /**
         * 分流：根据指定的Key的hashcode将元素发送到不同的分区，
         * 相同的Key会被分到一个分区（这里分区指的就是下游算子多个并行节点的其中一个）。
         * keyBy()是通过哈希来分区的
         */

//        默认的 MAX_PARALLELISM = 128
//        key会经过两次hash（hashcode， murmurhash），并与 MAX_PARALLELISM 取模 得到一个 ID
        // 发往下游哪个并行子任务，由selectChannel()决定 = id * 并行度 / MAX_PARALLELISM

        resultKS.print();
        senv.execute();

    }

    private class MyKeySelecor implements KeySelector<WaterSensor,String> {
        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }


    @Test
    public void transfrom_shuffer() throws Exception {
        senv.setParallelism(2);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");

        fileDS.print("input");

        // shuffle	打乱重组（洗牌）：将数据随机分布打散到下游
        DataStream<String> shuffleDS = fileDS.shuffle();

        shuffleDS.print("shuffle");

        senv.execute();

    }

    @Test
    public void transfrom_split_select() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");

        SingleOutputStreamOperator<WaterSensor> sensorDS = fileDS.map(file -> {
            String[] datas = file.split(",");
            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        });

        // 将数据流根据某些特征拆分成两个或者多个数据流，给不同数据流增加标记以便于从流中取出
        // vc <50 正常，50<vc<80警告，vc>80 告警
        SplitStream<WaterSensor> splitSS = sensorDS.split(new OutputSelector<WaterSensor>() {
            @Override
            public Iterable<String> select(WaterSensor value) {
                if (value.getVc() < 50) {
                    return Arrays.asList("noraml","baoku");
                } else if (value.getVc() < 80) {
                    return Arrays.asList("warn");
                } else {
                    return Arrays.asList("alarm");
                }
            }
        });
        // 从流中将不同的标记取出呢，这时需要使用select算子
        splitSS.select("baoku").print("noraml");
        splitSS.select("warn").print("warn");
        splitSS.select("alarm").print("alarm");


        senv.execute();

    }

    @Test
    public void transfrom_connect() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");

        SingleOutputStreamOperator<WaterSensor> sensorDS = fileDS.map(file -> {
            String[] datas = file.split(",");
            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        });

        DataStreamSource<Integer> numDS = senv.fromCollection(Arrays.asList(1, 2, 3, 4));

        /**
         * connect 流的连接
         * 1 只能对两条流进行连接
         * 2 数据类型可以不一样
         * 3 各自处理各自的数据
         */
        ConnectedStreams<WaterSensor, Integer> resultCS = sensorDS.connect(numDS);

        resultCS.map(new CoMapFunction<WaterSensor, Integer, Object>() {
            @Override
            public Object map1(WaterSensor value) throws Exception {
                return value.toString();
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value*2;
            }
        }).print();

        senv.execute();
    }

    @Test
    public void transfrom_union() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");

        SingleOutputStreamOperator<WaterSensor> sensorDS = fileDS.map(file -> {
            String[] datas = file.split(",");
            return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        });

        DataStreamSource<Integer> numDS1 = senv.fromCollection(Arrays.asList(1, 2, 3, 4));
        DataStreamSource<Integer> numDS2 = senv.fromCollection(Arrays.asList(5, 6, 7, 8));
        DataStreamSource<Integer> numDS3 = senv.fromCollection(Arrays.asList(9, 10));

        /**
         * union 流的连接
         * 1 流的数据类型必须一致
         * 2 可以连接多条流，得到的结果还是一个普通的DataStream
         * 3 各自处理各自的数据
         */
        // sensorDS.union(numDS);  报错
        DataStream<Integer> unionDS = numDS1.union(numDS2).union(numDS3);
        unionDS.print();


        senv.execute();
    }


}



