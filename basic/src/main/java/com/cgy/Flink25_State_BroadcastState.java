package com.cgy;

import javafx.scene.effect.SepiaTone;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author GyuanYuan Cai
 * 2020/11/2
 * Description:
 */

// 广播：
//应用场景：①动态配置(比读配置文件效率更高) ②规则
public class Flink25_State_BroadcastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        DataStreamSource<String> inputDS = senv.socketTextStream("localhost", 9999);
        DataStreamSource<String> controlDS = senv.socketTextStream("localhost", 8888);

        // TODO 1 把其中一条流（规则、配置文件，数据量小，新增及变化小的）广播
        MapStateDescriptor<String, String> broadcastStateDesc = new MapStateDescriptor<>("broadcast-state", String.class, String.class);
        BroadcastStream<String> controlBS = controlDS.broadcast(broadcastStateDesc);

        // TODO 2 将 主流A 与广播流 连接起来
        BroadcastConnectedStream<String, String> dataBCS = inputDS.connect(controlBS);

        // TODO 3
        dataBCS.process(new BroadcastProcessFunction<String, String, String>() {

            // 主流里的数据 处理
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 主流 只能获取使用 广播状态（只读） 不能对其进行修改
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastStateDesc);
                out.collect("主流的数据="+value+",广播状态的值="+broadcastState.get("switch"));
            }

            // 广播流里的数据 处理 定义广播状态里放什么数据
            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(broadcastStateDesc);
                String key = "switch";
                if ("1".equals(value)) {
                    broadcastState.put(key,"true");
                } else {
                    broadcastState.put(key,"false");
                }
            }
        }).print();

        senv.execute();

    }

}