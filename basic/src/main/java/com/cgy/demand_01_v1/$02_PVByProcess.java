package com.cgy.demand_01_v1;

import com.cgy.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author GyuanYuan Cai
 * 2020/10/28
 * Description:
 */

// 需求一： 网站总浏览量（PV）的统计
public class $02_PVByProcess {

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
        // 3.2 按照 pv行为 分组
        KeyedStream<UserBehavior, String> userBehaviorKS = filterDS.keyBy(data -> data.getBehavior());
        // 3.3 使用 process 进行 计数统计
        userBehaviorKS.process(new KeyedProcessFunction<String, UserBehavior, Object>() {

            private long pvCount;

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Object> out) throws Exception {
                pvCount++;
                out.collect(pvCount);
            }
        }).print();

        // 4 执行
        senv.execute();
    }

}


/**
 * Description:
 */

