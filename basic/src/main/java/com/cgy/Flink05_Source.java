package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * @author GyuanYuan Cai
 * 2020/10/27
 * Description:
 */

public class Flink05_Source {

    StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();


    @Test
    public void readList() throws Exception {

        DataStreamSource<Integer> inputDS = senv.fromCollection(Arrays.asList(1, 2, 3, 4));
        inputDS.print();

        DataStreamSource<WaterSensor> collectionDS = senv.fromCollection(
                Arrays.asList(
                        new WaterSensor("ws_001", 1577844001L, 45),
                        new WaterSensor("ws_002", 1577844015L, 43),
                        new WaterSensor("ws_003", 1577844020L, 42)
                )

        );
        collectionDS.print();


        senv.execute();
    }

    @Test
    public void readHdfsFile() throws Exception {
        // 要导入hdfs的依赖
        DataStreamSource<String> hdfsFile = senv.readTextFile("hdfs://hadoop102:9000/test/1.txt");
        hdfsFile.print();
    }


    @Test
    public void readKafka() throws Exception{  // 要引入kafka连接器依赖
        senv.setParallelism(1);

        // 从Kafka读取
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "flinkconsumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaDS = senv.addSource(new FlinkKafkaConsumer011<String>(
                "sensor",
                new SimpleStringSchema(),
                properties
        ));

        kafkaDS.print("kafka source");

        senv.execute();
    }


    @Test
    public void readDIYSource() throws Exception{
        senv.setParallelism(1);
        //  自定义数据源
        DataStreamSource<WaterSensor> inputDS = senv.addSource(new MySource());
        inputDS.print();
        senv.execute();
    }

    /**
     * 自定义Source
     * 1. 实现 Source Function，指定产生数据的类型
     * 2. 重写 run 和 cancel方法
     */
    public static class MySource implements SourceFunction<WaterSensor>{

        boolean flag = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                ctx.collect(
                    new WaterSensor(
                            "sensor_"+(random.nextInt(3)+1),
                            System.currentTimeMillis(),
                            random.nextInt(10)+40
                    )
                );
            }
        }

        @Override
        public void cancel() {
            flag=false;
        }
    }
}



