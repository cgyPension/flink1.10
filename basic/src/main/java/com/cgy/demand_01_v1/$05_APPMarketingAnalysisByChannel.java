package com.cgy.demand_01_v1;

import com.cgy.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author GyuanYuan Cai
 * 2020/10/28
 * Description:
 */

// APP市场推广统计 - 分渠道
public class $05_APPMarketingAnalysisByChannel {

    public static void main(String[] args) throws Exception {

            // 1 创建执行环境
            StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
            senv.setParallelism(1);
            // 2 读取数据
            DataStreamSource<MarketingUserBehavior> appDS = senv.addSource(new APPMarketingDataSource());
            // 3 处理数据
            // 3.1 按照 统计维度（渠道、行为） 分组
            KeyedStream<Tuple2<String, Integer>, String> channelBehaviorAndOneKS = appDS.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                    return Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1);
                }
            }).keyBy(data -> data.f0);
            // 3.2 聚合
            SingleOutputStreamOperator<Tuple2<String, Integer>> redultDS = channelBehaviorAndOneKS.sum(1);

            redultDS.print();

            senv.execute();


    }

    // 由于没有现成的数据，所以我们需要自定义一个测试源来生成用户行为的事件流。
    public static class APPMarketingDataSource implements SourceFunction<MarketingUserBehavior>{
        private static boolean flag = true;
        private List<String> behaviorList=Arrays.asList("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL");
        private List<String> channelList=Arrays.asList("XIAOMI", "HUAWEI", "OPPO", "VIVO");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                ctx.collect(
                        new MarketingUserBehavior(
                                random.nextLong(),
                                behaviorList.get(random.nextInt(behaviorList.size())),
                                channelList.get(random.nextInt(channelList.size())),
                                System.currentTimeMillis()
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}


/**
 * Description:
 */

