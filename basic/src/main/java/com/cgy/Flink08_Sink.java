package com.cgy;

import com.cgy.bean.WaterSensor;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.hadoop2.org.apache.http.HttpHost;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.junit.Test;
import sun.management.Sensor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author GyuanYuan Cai
 * 2020/10/28
 * Description:
 */

public class Flink08_Sink {
    StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void Sink_kafka() throws Exception {
        senv.setParallelism(1);
        //DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");
        DataStreamSource<String> fileDS = senv.socketTextStream("hadoop102",9999);

        // TODO ksfka Sink
        fileDS.addSink(new FlinkKafkaProducer011<String>(
                "hadoop102:9092,hadoop103:9092,hadoop104:9092",
                "sensor0523",
                new SimpleStringSchema()
        ));

        senv.execute();
    }

    @Test
    public void Sink_redis() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");
        //DataStreamSource<String> fileDS = senv.socketTextStream("hadoop102",9999);

        // TODO redis Sink
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        fileDS.addSink(new RedisSink<>(conf, new RedisMapper<String>() {
            // 指定 redis 的操作命令，和最外层的key
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"sensor0523");
            }

            // 从数据中获取 key：在Hash中，就是 Hash的key
            @Override
            public String getKeyFromData(String data) {
                return data.split(",")[1];
            }

            // 从数据中获取 value：在Hash中，就是 Hash的value
            @Override
            public String getValueFromData(String data) {
                return data.split(",")[2];
            }
        }));

        senv.execute();

    }

    @Test
    public void Sink_ES() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");
        //DataStreamSource<String> fileDS = senv.socketTextStream("hadoop102",9999);

        // TODO ES Sink
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        httpHosts.add(new HttpHost("hadoop103",9200));
        httpHosts.add(new HttpHost("hadoop104",9200));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        HashMap<String, String> dataMap = new HashMap<>();
                        dataMap.put("data", element);
                        // TODO ES 中的索引不能是大写 否则报错
                        IndexRequest indexRequest = Requests.indexRequest("sensor0523").type("readingabc").source(dataMap);
                        ;
                        indexer.add(indexRequest);
                    }
                }
        );
        // 设置 bulk 的 数量为 1就进行刷写
        // 生产环境中 不需要设置 这是ES缓存优化 多条才写入
        esSinkBuilder.setBulkFlushMaxActions(1);

        fileDS.addSink(esSinkBuilder.build());

        senv.execute();

    }

    @Test
    public void Sink_DIY_MySQL() throws Exception {
        senv.setParallelism(1);
        DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");
        //DataStreamSource<String> fileDS = senv.socketTextStream("hadoop102",9999);

        // TODO DIY_MySQL Sink flink原本没有提供Mysql
        fileDS.addSink(new MySQLSink());

        senv.execute();

    }


    private static class MySQLSink extends RichSinkFunction<String> {

        private Connection conn;
        private PreparedStatement pstmt;

        // 初始化 连接对象 和 预编译对象
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            pstmt = conn.prepareStatement("INSERT INTO sersor values (?,?,?)");
        }

        @Override
        public void close() throws Exception {
            pstmt.close();
            conn.close();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            String[] sensorStrs = value.split(",");
            pstmt.setString(1,sensorStrs[0]);
            pstmt.setLong(2,Long.valueOf(sensorStrs[1]));
            pstmt.setInt(3,Integer.valueOf(sensorStrs[2]));

            pstmt.execute();
        }
    }
}



