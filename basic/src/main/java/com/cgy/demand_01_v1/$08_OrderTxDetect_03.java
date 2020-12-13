package com.cgy.demand_01_v1;

/**
 * @author GyuanYuan Cai
 * 2020/10/30
 * Description:
 */

import com.cgy.bean.OrderEvent;
import com.cgy.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 订单数据从OrderLog.csv中读取 交易数据从ReceiptLog.csv中读取
 *  数据格式：
 *  Long orderId, String eventType, String txId, Long eventTime
 *
 *  String txId, String payChannel, Long eventTime
 *
 *  我的思路：
 *  要转换格式吗？(txId,(orderEvent))
 *  实时对账	来自两条流的订单交易匹配 用equals 匹配存进list集合
 *  txId匹配上就可以,然后返回订单ID
 *
 *  三种方法：
 *  ①  没有并行度/并行度为1
 *  ② 设置并行度，先keyby后connect（如果不keyBy、connect 数据会分开 36+2）
 *  ③  设置并行度，先connect后keyby 效果和②一样
 */

public class $08_OrderTxDetect_03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(2);

        SingleOutputStreamOperator<OrderEvent> orderDS = senv.readTextFile("basic/input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
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
                });

        SingleOutputStreamOperator<TxEvent> txDS = senv.readTextFile("basic/input/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new TxEvent(
                                datas[0],
                                datas[1],
                                Long.valueOf(datas[2])
                        );
                    }
                });

        // 关联两条流，使用 connect
        ConnectedStreams<OrderEvent, TxEvent> orderTxCS = orderDS.connect(txDS);

        // 使用 ProcessFunction
        SingleOutputStreamOperator<String> redultDS = orderTxCS.keyBy(order -> order.getTxId(), tx -> tx.getTxId())//连接两条流，如果使用连接条件进行匹配，要对 连接条件 进行keyby，避免多并行度的影响
                .process(new OrderTxDetectFunction());

        redultDS.print();

        senv.execute();

    }


    private static class OrderTxDetectFunction extends CoProcessFunction<OrderEvent,TxEvent,String> {
        private Map<String, TxEvent> txEventMap = new HashMap<>();
        private Map<String, OrderEvent> orderMap = new HashMap<>();

        //  TODO 处理 业务系统的 数据：一条一条处理
        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            // 进入这个方法，说明处理的是 业务系统的 数据
            // 判断，同一个交易码的交易系统的数据 来过没有？
            TxEvent txEvent = txEventMap.get(value.getTxId());
            if (txEvent == null) {
                // 1.同一个交易码的 交易系统的数据，没来过 => 把 自己（业务系统的数据） 保存起来，等待 交易系统的数据
                orderMap.put(value.getTxId(),value);
            } else {
                // 2.同一个交易码的 交易系统的数据，来过 => 匹配上
                out.collect("订单"+value.getOrderId()+"通过交易码="+value.getTxId()+"对账成功！！！");
                txEventMap.remove(value.getTxId());
            }

        }

        // TODO 处理 交易系统的 数据：一条一条处理
        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
            // 进入这个方法，说明处理的是 交易系统的 数据
            // 判断，同一个交易码的业务系统的数据 来过没有？
            OrderEvent orderEvent = orderMap.get(value.getTxId());
            if (orderEvent == null) {
                // 1.同一个交易码的 业务系统的数据，没来过 => 把 自己（交易系统的数据） 保存起来，等待 业务系统的数据
                txEventMap.put(value.getTxId(),value);
            } else {
                // 2.同一个交易码的 业务系统的数据，来过 => 匹配上
                out.collect("订单"+orderEvent.getOrderId()+"通过交易码="+value.getTxId()+"对账成功！！！");
                orderMap.remove(value.getTxId());
            }
        }
    }
}


/**
 * Description:
 */

