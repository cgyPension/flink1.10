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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import scala.Int;

import javax.annotation.Nullable;

/**
 * @author GyuanYuan Cai
 * 2020/11/2
 * Description:
 */

public class Flink26_State_StateBackend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // TODO 执行环境 设置 状态后端
        /**
         *  1 memory
         * 内存级的状态后端，会将键控状态作为内存中的对象进行管理，
         * 将它们存储在TaskManager的JVM堆上；而将checkpoint存储在JobManager的内存中。
         */
        MemoryStateBackend memoryStateBackend = new MemoryStateBackend();
        senv.setStateBackend(memoryStateBackend);

        /**
         * 2 fs
         * 将checkpoint存到远程的持久化文件系统（FileSystem）上。
         * 而对于本地状态，跟MemoryStateBackend一样，也会存在TaskManager的JVM堆上。
         */
        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://xxx/xxx");
        senv.setStateBackend(fsStateBackend);

        /**
         * 3 RocksDB
         * 将所有状态序列化后，存入本地的RocksDB中存储；
         * Checkpoint保存在持久化文件系统，比如HDFS
         * 要单独导依赖
         */
        StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://xxx/xxx");
        senv.setStateBackend(rocksDBStateBackend);

        // TODO 2 打开checkpoint
        senv.enableCheckpointing(3000L);

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