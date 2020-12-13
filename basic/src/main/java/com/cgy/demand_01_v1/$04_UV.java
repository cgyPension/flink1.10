package com.cgy.demand_01_v1;

import com.cgy.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @author GyuanYuan Cai
 * 2020/10/28
 * Description:
 */

//  网站独立访客数（UV）的统计
//  对于UserBehavior数据源来说，我们直接可以根据userId来区分不同的用户

/*
 *@Description:统计UV,同一个用户的浏览行为会被重复统计,而在实际应用中,我们往往
 * 还会关注,到低有多少不同的用户访问了这个网站,所以另外一个统计流量的重要指标是网站的独立访客数
 * 需求分析:需要考虑去重的操作
 * 去重的方法:
 * (1)利用HashSet,把数据存入HashSet中,重复的数据会被覆盖
 * (2)利用redis,把数据存入redis中,重复的数据存不进去
 * (3)使用布隆过滤器,判断元素存在 则不一定存在,判断元素不存在,则一定不存在
 *  * 543462,  1715,   1464116,   pv,     1511658000
 * 用户ID、商品ID、商品类目ID、行为类型 ,时间戳
 *--->215662
 */

public class $04_UV {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        // 2 读取数据
        SingleOutputStreamOperator<UserBehavior> userbehaviorDS = senv.readTextFile("basic/input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                });

        // 3 处理数据
        // 3.1 过滤 => 只保留 pv行为
        SingleOutputStreamOperator<UserBehavior> filterDS = userbehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        // 3.2 转换成 （"uv"，用户ID）格式
        // => uv是为了分组用
        // => 用户ID，是为了放入Set去重， 其他的字段不关心，不需要
        SingleOutputStreamOperator<Tuple2<String, Long>> uvAndUserIdDS = filterDS.map(
                new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return Tuple2.of("uv", value.getUserId());
                    }
                }
        );
        // 3.3 按照 uv 分组
        KeyedStream<Tuple2<String, Long>, String> uvAndUserIDKS = uvAndUserIdDS.keyBy(data -> data.f0);

        SingleOutputStreamOperator<Long> uvDS = uvAndUserIDKS.process(new KeyedProcessFunction<String, Tuple2<String, Long>, Long>() {
            // 定义一个Set，用来存储 userID
            private Set<Long> userIdSet = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Long> out) throws Exception {
                // 每来一条数据，就把 userID 放入 Set中
                userIdSet.add(value.f1);
                out.collect(Long.valueOf(userIdSet.size()));
            }
        });

        // 4 输出
        uvDS.print();
        // 5 执行
        senv.execute();
    }

}


