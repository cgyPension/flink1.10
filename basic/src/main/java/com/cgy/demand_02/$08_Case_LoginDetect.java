package com.cgy.demand_02;

/**
 * @author GyuanYuan Cai
 * 2020/11/6
 * Description:
 */


import com.cgy.bean.LoginEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 恶意登录监控 2秒内 连续 2次登录失败
 * 就认为存在恶意登录的风险，输出相关的信息进行报警提示。这是电商网站、也是几乎所有网站风控的基本一环。
 *
 *  样例类：
 *  Long userId, String ip, String eventType, Long eventTime
 *
 *  1 读取数据
 *
 *      乱序10秒
 *      不能过滤因为 要连续
 *
 *  2 业务逻辑
 *      按照用户维度分组
 *      不能用窗口，因为怎样都会有漏计算，用定时器(还能知道频率)
 *      process、
 *      要用一个ListState状态存失败的数据，在open里做初始化
 *      判断来的数据事件类型 1 如果是成功的数据 不管满不满足条件 都要删除定时器、listState
 *      1.1 判断一下listState里面存了几个失败数据，if>=2 则告警
 *     2如果是失败的数据添加到ListState，并且注册一个2秒的定时器
 *
 *      F
 *      S
 *      F
 *
 *     触发定时器：
 *     listState>=2 告警
 *     清空集合、定时器
 *
 *  3 执行
 */
public class $08_Case_LoginDetect {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 读取数据
        SingleOutputStreamOperator<LoginEvent> loginDS = senv
                .readTextFile("basic/input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new LoginEvent(
                                Long.valueOf(datas[0]),
                                datas[1],
                                datas[2],
                                Long.valueOf(datas[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(LoginEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );

        KeyedStream<LoginEvent, Long> loginKS = loginDS.keyBy(data -> data.getUserId());

        loginKS.process(new KeyedProcessFunction<Long, LoginEvent, String>() {

            ListState<LoginEvent> failLogins;
            ValueState<Long> timerTs;

            @Override
            public void open(Configuration parameters) throws Exception {
                failLogins = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("failLogins", LoginEvent.class));
                timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs",Long.class));
            }

            //Long userId, String ip, String eventType, Long eventTime
            @Override
            public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
                // 判断来的数据是成功还是失败
                if ("success".equals(value.getEventType())) {
                    // 1 来的是 成功 的数据
                    // 1.1 不管满不满足条件，都要删除定时器
                    if (timerTs.value()!=null) {
                        ctx.timerService().deleteEventTimeTimer(timerTs.value());
                    }
                    timerTs.clear();
                    // 1.2 判断一下 listState里面存了几个失败数据
                    if (failLogins.get().spliterator().estimateSize()>=2) {// estimateSize会预估、有时候会不准确
                        out.collect("用户"+value.getUserId()+"在2s内，连续登陆失败次数超过阈值，可能存在风险！！！");
                    }
                    failLogins.clear();
                } else {
                    // 2.来的是 失败 的数据 => 添加到 ListState，并且注册一个 2s的定时器
                    failLogins.add(value);
                    if (timerTs.value() == null) {
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp()+2000L);
                        timerTs.update(ctx.timestamp()+2000L);
                    }

                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                // 要迭代 计算
                Iterable<LoginEvent> loginEvents = failLogins.get();
                Long count = 0L;
                for (LoginEvent loginEvent : loginEvents) {
                    count++;
                }

                if (count >= 2) {
                    out.collect("用户"+ctx.getCurrentKey()+"在2s内，连续登陆失败"+count+"次，超过阈值2，可能存在风险！！！");
                }

                failLogins.clear();
                timerTs.clear();
            }

        }).print();


        senv.execute();
    }

}






















