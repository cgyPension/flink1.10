package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

/**
 * @author GyuanYuan Cai
 * 2020/11/2
 * Description:
 */

public class Flink27_Checkpoint_Config {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * TODO checkpoint
         * 1.算法名称： Chandy-Lamport(昌迪兰伯特) 分布式快照算法
         * 生成一个barrier，插入到source前面，随着数据流的流动往下传递；
         * 每个task，接收到 barrier的时候，会触发 快照操作（保存到哪看 状态后端）
         * 每个task，完成快照操作后，会通知 JM
         * 当所有的task都完成了 快照，JM会通知：本次ck完成 => 2pc中的正式提交
         *
         * 2. barrier对齐
         * 假设 source 并行度=2，下游的map并行度 = 1
         * map先接收到 source1的 bariier，会停止处理source1后续的数据，放在缓冲区中
         * 当 map接收到 source2（也就是所有的barrier）时，触发 map的 状态备份
         * 备份完成后，处理 缓冲区中的数据， 之后接着往下处理
         *
         *
         * 3. barrier不对齐
         * map先接收到 source1的 bariier，不停止处理source1后续的数据
         *
         * barrier对齐才能 精准一次，不对齐只能 at-least-once
         *
         */

        //TODO checkpoint常用配置项
        senv.enableCheckpointing(3000L); // 开启ck，设置周期 每两个 barrier产生的间隔
        senv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        senv.getCheckpointConfig().setCheckpointTimeout(300000L); // ck执行多久超时
        senv.getCheckpointConfig().setMaxConcurrentCheckpoints(2); // 异步ck，同时有几个ck开启
        senv.getCheckpointConfig().setPreferCheckpointForRecovery(false); // 默认为false 从 ck 恢复；true 从savepoint恢复
        senv.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); // 允许当前checkpoint失败的次数

        SingleOutputStreamOperator<WaterSensor> seneorDS = senv.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                }).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<WaterSensor>() {// 没有时间周期规律生成watermarks
                    @Override
                    public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                        return new Watermark(extractedTimestamp);
                    }

                    @Override
                    public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<String> resultDS = seneorDS.keyBy(seneor -> seneor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 1 定义状态
                    ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 2 在open中初始化状态
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("当前key=" + ctx.getCurrentKey() + ",值状态保存的上一次水位值=" + valueState.value());
                        // 保存上一条数据的水位值，保存到状态里
                        valueState.update(value.getVc());
                    }
                });

        resultDS.print();

        senv.execute();

    }

}