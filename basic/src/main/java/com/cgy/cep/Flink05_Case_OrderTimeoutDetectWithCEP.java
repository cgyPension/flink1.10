package com.cgy.cep;

import com.cgy.bean.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.List;
import java.util.Map;

/**
 * @author GyuanYuan Cai
 * 2020/11/6
 * Description:
 */

// 订单支付超时 15分钟

public class Flink05_Case_OrderTimeoutDetectWithCEP {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.1 读取 业务系统的 数据
        SingleOutputStreamOperator<OrderEvent> orderDS = env
                .readTextFile("basic/input/OrderLog.csv")
                .map(
        // 1. 读取数据
                        new MapFunction<String, OrderEvent>() {
                            @Override
                            public OrderEvent map(String value) throws Exception {
                                String[] datas = value.split(",");
                                return new OrderEvent(
                                        Long.valueOf(datas[0]),
                                        datas[1],
                                        datas[2],
                                        Long.valueOf(datas[3])
                                );
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<OrderEvent>() {
                            @Override
                            public long extractAscendingTimestamp(OrderEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );


        // 2. 处理数据
        // 2.1 按照 统计维度 分组：订单
        KeyedStream<OrderEvent, Long> orderKS = orderDS.keyBy(data -> data.getOrderId());

        // TODO 使用CEP实现
        // 1.定义规则
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.milliseconds(15 * 60 * 1000 + 1));
        //.within(Time.minutes(15));

        // 2.应用规则
        PatternStream<OrderEvent> orderPS = CEP.pattern(orderKS, pattern);

        // 3.获取结果:三个参数
        // 第一个参数：OutputTag， 超时的数据会放入侧输出流，所以需要一个 侧输出流标签
        // 第二个参数：PatternTimeoutFunction，超时数据的处理函数，会对超时的数据进行处理，之后 放入 侧输出流
        // 第三个参数：PatternSelectFunction，对匹配上的数据进行处理，输出到 主流
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };

        SingleOutputStreamOperator<String> redultDS = orderPS.select(
                timeoutTag,
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.toString() + "---->" + timeoutTimestamp;
                    }
                }, new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        return pattern.toString();
                    }
                }
        );

        //redultDS.print("normal");
        redultDS.getSideOutput(timeoutTag).print("timeout");
        env.execute();
    }

}