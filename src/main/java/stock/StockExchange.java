package stock;

import Nexmark.sinks.DummySink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class StockExchange {
    private static final String INPUT_STREAM_ID = "stock_sb";
    private static final String OUTPUT_STREAM_ID = "stock_cj";
    private static final String KAFKA_BROKERS = "localhost:9092";

    private static final String FILTER_KEY1 = "D";
    private static final String FILTER_KEY2 = "X";
    private static final String FILTER_KEY3 = "";

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        FlinkKafkaProducer011<Tuple2<String, String>> kafkaProducer = new FlinkKafkaProducer011<Tuple2<String, String>>(
                KAFKA_BROKERS, INPUT_STREAM_ID, new KafkaWithTsMsgSchema());
        kafkaProducer.setWriteTimestampToKafka(true);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", params.get("kafka", KAFKA_BROKERS));
        FlinkKafkaConsumer011<Tuple2<String, String>> inputConsumer = new FlinkKafkaConsumer011<>(
                INPUT_STREAM_ID, new KafkaWithTsMsgSchema(), kafkaProps);

        inputConsumer.setStartFromLatest();
        inputConsumer.setCommitOffsetsOnCheckpoints(false);

        final DataStream<Tuple2<String, String>> text = env.addSource(
                inputConsumer);

        // split up the lines in pairs (2-tuples) containing:
        DataStream<Tuple2<String, String>> counts = text.keyBy(0)
                .flatMap(new MatchMaker())
                .name("Splitter FlatMap")
                .uid("flatmap")
                .setParallelism(params.getInt("p2", 3))
                .keyBy(0);

        counts.addSink(kafkaProducer)
                .name("Sink")
                .uid("sink")
                .setParallelism(params.getInt("p3", 1));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        // execute program
        env.execute("Stateful WordCount");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static final class MatchMaker implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {
        private static final long serialVersionUID = 1L;

        private Map<String, Map<Float, List<Order>>> pool = new HashMap<>();
        private Map<String, List<Float>> poolPrice = new HashMap<>();

        @Override
        public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, String>> out) throws Exception {

            String stockOrder = (String) value.f1;
            String[] orderArr = stockOrder.split("\\|");
            Order order = new Order(stockOrder);

            List<String> xactResult = stockExchange(pool, poolPrice, order);

            out.collect(new Tuple2<>(value.f0, value.f1));
        }

        /**
         * deal continous transaction
         * @param poolB,poolS,pool,order
         * @return output string
         */
        private List<String> transaction(Map<Float, List<Order>> poolB, Map<Float, List<Order>> poolS,
                                         List<Float> poolPriceB, List<Float> poolPriceS, Map<String, List<Float>> poolPrice,
                                         Map<String, Map<Float, List<Order>>> pool, Order order) {
            // hava a transaction
            int top = 0;
            int i = 0;
            int j = 0;
            int otherOrderVol;
            int totalVol = 0;
            float tradePrice = 0;
            List<String> tradeResult = new ArrayList<>();
            while (poolPriceS.get(top) <= poolPriceB.get(top)) {
                tradePrice = poolPriceS.get(top);
                if (poolB.get(poolPriceB.get(top)).get(top).getOrderVol() > poolS.get(poolPriceS.get(top)).get(top).getOrderVol()) {
                    // B remains B_top-S_top
                    otherOrderVol = poolS.get(poolPriceS.get(top)).get(top).getOrderVol();
                    // totalVol sum
                    totalVol += otherOrderVol;
                    poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
                    // S complete
                    poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
                    // remove top of poolS
                    poolS.get(poolPriceS.get(top)).remove(top);
                    // no order in poolS, transaction over
                    if (poolS.get(poolPriceS.get(top)).isEmpty()) {
                        // find next price
                        poolS.remove(poolPriceS.get(top));
                        poolPriceS.remove(top);
                        if (poolPriceS.isEmpty()) {
                            break;
                        }
                    }
                    // TODO: output poolB poolS price etc
                } else {
                    otherOrderVol = poolB.get(poolPriceB.get(top)).get(top).getOrderVol();
                    // totalVol sum
                    totalVol += otherOrderVol;
                    poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
                    poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
                    poolB.get(poolPriceB.get(top)).remove(top);
                    // no order in poolB, transaction over
                    if (poolB.get(poolPriceB.get(top)).isEmpty()) {
                        poolB.remove(poolPriceB.get(top));
                        poolPriceB.remove(top);
                        if (poolPriceB.isEmpty()) {
                            break;
                        }
                    }
                    // TODO: output poolB poolS price etc
                }
            }
            pool.put(order.getSecCode()+"S", poolS);
            pool.put(order.getSecCode()+"B", poolB);
            poolPrice.put(order.getSecCode()+"B", poolPriceB);
            poolPrice.put(order.getSecCode()+"S", poolPriceS);
            tradeResult.add(order.getSecCode());
            tradeResult.add(String.valueOf(totalVol));
            tradeResult.add(String.valueOf(tradePrice));
            // return tradeResult;
            return tradeResult;
        }

        /**
         * mapFunction
         * @param pool, order
         * @return String
         */
        private List<String> stockExchange(Map<String, Map<Float, List<Order>>> pool, Map<String, List<Float>> poolPrice, Order order) {
            // String complete = new String();
            List<String> tradeResult = new ArrayList<>();
            // load poolS poolB
            Map<Float, List<Order>> poolS = pool.get(order.getSecCode()+"S");
            Map<Float, List<Order>> poolB = pool.get(order.getSecCode()+"B");
            List<Float> poolPriceB = poolPrice.get(order.getSecCode()+"B");
            List<Float> poolPriceS = poolPrice.get(order.getSecCode()+"S");

            if (poolB == null) {
                poolB = new HashMap<>();
            }
            if (poolS == null) {
                poolS = new HashMap<>();
            }
            if (poolPriceB == null) {
                poolPriceB = new ArrayList<>();
            }
            if (poolPriceS == null) {
                poolPriceS = new ArrayList<>();
            }

            if (order.getTradeDir().equals("B")) {
                float orderPrice = order.getOrderPrice();
                List<Order> BorderList = poolB.get(orderPrice);
                // if order tran_maint_code is "D", delete from pool
                if (FILTER_KEY1.equals(order.getTranMaintCode())) {
                    // if exist in order, remove from pool
                    String orderNo = order.getOrderNo();
                    if (BorderList == null) {
                        // return "{\"process_no\":\"11\", \"result\":\"no such B order to delete:" + orderNo+"\"}";
                        return tradeResult;
                    }
                    for (int i=0; i < BorderList.size(); i++) {
                        if (orderNo.equals(BorderList.get(i).getOrderNo())) {
                            BorderList.remove(i);
                            // if no other price delete poolPrice
                            if (BorderList.isEmpty()) {
                                for (int j=0; j < poolPriceB.size(); j++) {
                                    if (poolPriceB.get(j) == orderPrice) {
                                        poolPriceB.remove(j);
                                        break;
                                    }
                                }
                                poolB.remove(orderPrice);
                            } else {
                                poolB.put(orderPrice, BorderList);
                            }
                            poolPrice.put(order.getSecCode()+"B", poolPriceB);
                            pool.put(order.getSecCode()+"B", poolB);
                            return tradeResult;
                        }
                    }
                    // else output no delete order exist
                    return tradeResult;
                }

                // put into buy poolB
                if (BorderList == null) {
                    BorderList = new ArrayList<>();
                    // price add a value
                    if (poolB.isEmpty()) {
                        poolPriceB.add(orderPrice);
                    } else {
                        for (int i = 0; i < poolPriceB.size(); i++) {
                            if (poolPriceB.get(i) < orderPrice) {
                                poolPriceB.add(i, orderPrice);
                                break;
                            }
                            if (i == poolPriceB.size()-1) {
                                poolPriceB.add(orderPrice);
                                break;
                            }
                        }
                    }
                }
                BorderList.add(order);
                poolB.put(orderPrice, BorderList);

                // if no elements in poolS, no transaction, add poolB
                if (poolPriceS.isEmpty()) {
                    pool.put(order.getSecCode()+"B", poolB);
                    poolPrice.put(order.getSecCode()+"B", poolPriceB);
                    // return complete;
                    return tradeResult;
                }

                // no satisfied price
                if (poolPriceS.get(0) > poolPriceB.get(0)) {
                    // this.savepool();
                    pool.put(order.getSecCode()+"S", poolS);
                    pool.put(order.getSecCode()+"B", poolB);
                    poolPrice.put(order.getSecCode()+"S", poolPriceS);
                    poolPrice.put(order.getSecCode()+"B", poolPriceB);
                    return tradeResult;
                } else {
                    tradeResult = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
                }
            } else if (order.getTradeDir().equals("S")) {
                float orderPrice = order.getOrderPrice();
                List<Order> SorderList = poolS.get(orderPrice);
                // if order tran_maint_code is "D", delete from pool
                if (FILTER_KEY1.equals(order.getTranMaintCode())) {
                    // if exist in order, remove from pool
                    String orderNo = order.getOrderNo();
                    if (SorderList == null) {
                        return tradeResult;
                    }
                    for (int i=0; i < SorderList.size(); i++) {
                        if (orderNo.equals(SorderList.get(i).getOrderNo())) {
                            SorderList.remove(i);
                            // if no other price delete poolPrice
                            if (SorderList.isEmpty()) {
                                for (int j=0; j < poolPriceS.size(); j++) {
                                    if (poolPriceS.get(j) == orderPrice) {
                                        poolPriceS.remove(j);
                                        break;
                                    }
                                }
                                poolS.remove(orderPrice);
                            } else {
                                poolS.put(orderPrice, SorderList);
                            }
                            poolPrice.put(order.getSecCode()+"S", poolPriceS);
                            pool.put(order.getSecCode()+"S", poolS);
                            return tradeResult;
                        }
                    }
                    // else output no delete order exist
                    return tradeResult;
                }

                // put into buy poolS
                if (SorderList == null) {
                    SorderList = new ArrayList<>();
                    // price add a value
                    if (poolS.isEmpty()) {
                        poolPriceS.add(orderPrice);
                    } else {
                        for (int i = 0; i < poolPriceS.size(); i++) {
                            if (poolPriceS.get(i) > orderPrice) {
                                poolPriceS.add(i, orderPrice);
                                break;
                            }
                            if (i == poolPriceS.size()-1) {
                                poolPriceS.add(orderPrice);
                                break;
                            }
                        }
                    }
                }
                SorderList.add(order);
                poolS.put(orderPrice, SorderList);
                // if no elements in poolB, no transaction, add poolS
                if (poolPriceB.isEmpty()) {
                    pool.put(order.getSecCode()+"S", poolS);
                    poolPrice.put(order.getSecCode()+"S", poolPriceS);
                    return tradeResult;
                }

                // no satisfied price
                if (poolPriceS.get(0) > poolPriceB.get(0)) {
                    // this.savepool();
                    pool.put(order.getSecCode()+"S", poolS);
                    pool.put(order.getSecCode()+"B", poolB);
                    poolPrice.put(order.getSecCode()+"S", poolPriceS);
                    poolPrice.put(order.getSecCode()+"B", poolPriceB);
                    return tradeResult;
                } else {
                    tradeResult = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
                }
            }
            return tradeResult;
        }
    }
}
