package com.cgy;

import com.cgy.bean.WaterSensor;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

/**
 * @author GyuanYuan Cai
 * 2020/10/30
 * Description:
 */



public class Flink09_Window_IncreAgg_process {


    @Test
    public void Window_TimeWindow() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = senv
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                });

        // TODO 开窗
        // DataStream可以直接调用开窗的方法，但是都带"all",这种情况下所有 数据不分组，都在窗口里
//        socketDS.windowAll();
//        socketDS.countWindowAll();
//        socketDS.timeWindowAll();

        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(data -> data.f0);


        /**
         *  TODO TimeWindow API
         * 	TimeWindow：按照时间生成Window，根据窗口实现原理可以分成三类
         * 	滚动窗口（Tumbling Window）
         * 	滑动窗口（Sliding Window）
         * 	会话窗口（Session Window）
         */

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> wordAndOneWS = wordAndOneKS
//              .timeWindow(Time.seconds(5));// 滚动窗口 => 传一个参数： 窗口长度
//        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
               .timeWindow(Time.seconds(5),Time.seconds(2));// 滑动窗口 => 传两个参数： 第一个是 窗口长度 ； 第二个是 滑动步长
//        .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(2)));
//        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)));// 会话窗口

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneWS.sum(1);

        resultDS.print();
        senv.execute();
    }




    @Test
    public void Window_CountWindow() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = senv
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                });

        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(data -> data.f0);

        /**  TODO CountWindow API
         *  CountWindow：按照指定的数据条数生成一个Window，与时间无关
         * 	滚动窗口
         *  滑动窗口
         */
        // 分组之后再开窗，那么窗口的关闭是看，相同分组的数据条数是否达到
        // 在滑动窗口中，一个数据能属于多少个窗口？ => 窗口长度 / 滑动步长
        // 滑动窗口 => 每经过一个步长，就会触发一个窗口的计算
        // 在第一条数据之前，也是有窗口的，只不过是没有数据属于那个窗口
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> wordAndOneWS = wordAndOneKS
//                .countWindow(5)
                .countWindow(5, 2);
        wordAndOneWS.sum(1).print();
        senv.execute();

    }

    @Test
    public void Window_IncreAggFunction() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = senv
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                });

        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(data -> data.f0);


        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> wordAndOneWS = wordAndOneKS
                .timeWindow(Time.seconds(5)); // 滚动窗口

        /**
         * TODO 窗口的增量聚合函数
         * 每条数据到来就进行计算，保持一个简单的状态。典型的增量聚合函数有：
         * 直到窗口结束
         */

        wordAndOneWS
//                .reduce( // 来一条加一条
//                        new ReduceFunction<Tuple2<String, Integer>>() {
//                            @Override
//                            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//                                System.out.println(value1 + "<---->" + value2);
//                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
//                            }
//                        }
//                )

                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Long, Long>() {

                    // 初始化累加器，根据指定的累加器类型进行初始化
                    @Override
                    public Long createAccumulator() {
                        System.out.println("create....");
                        return 0L;
                    }

                    // 定义 聚合操作：每条数据 跟 累加器 的聚合行为
                    @Override
                    public Long add(Tuple2<String, Integer> value, Long accumulator) {
                        System.out.println("add...");
                        return value.f1+accumulator;
                    }

                    // 获取结果
                    @Override
                    public Long getResult(Long accumulator) {
                        System.out.println("get result...");
                        return accumulator;
                    }

                    // 合并不同窗口的累加器 => 会话窗口才会调用
                    @Override
                    public Long merge(Long a, Long b) {
                        System.out.println("merge...");
                        return a+b;
                    }
                })
                .print();




        senv.execute();

    }

    @Test
    public void Window_ProcessWindowFunction() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = senv
//                .readTextFile("input/sensor-data.log")
                .socketTextStream("hadoop102",9999 )
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });


        /**
         * ProcessWindowFunction
         * 全窗口函数：整个窗口的本组数据，存起来，关窗的时候一次性一起计算
         */

        sensorDS.keyBy(sensor-> sensor.getId())
                .timeWindow(Time.seconds(5))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override// <IN, OUT, KEY, W extends Window>
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                System.out.println("process...");
                                out.collect("当前key="+s+",一共有"+elements.spliterator().estimateSize()+ "条数据");
                            }
                        }
                ).print();



        senv.execute();

    }
}


/**
 * Description:
 */

