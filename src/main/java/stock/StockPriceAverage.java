package stock;

import Nexmark.sinks.DummySink;
import flinkapp.WordCount;
import flinkapp.wordcount.StatefulWordCount;
import flinkapp.wordcount.sources.RateControlledSourceFunctionKV;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

import static java.lang.Thread.sleep;

public class StockPriceAverage {
    private static final String INPUT_STREAM_ID = "stock_input";
    private static final String OUTPUT_STREAM_ID = "stock_average";
    private static final String KAFKA_BROKERS = "localhost:9092";

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        FlinkKafkaProducer011<String> kafkaProducer = new FlinkKafkaProducer011<String>(
                KAFKA_BROKERS, INPUT_STREAM_ID, new SimpleStringSchema());
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
        // (w`ord,1)
        DataStream<Tuple2<String, Long>> counts = text.keyBy(0)
                .flatMap(new Tokenizer())
                .name("Splitter FlatMap")
                .uid("flatmap")
                .setParallelism(params.getInt("p2", 3))
                .keyBy(0)
                .flatMap(new CountWords())
                .name("Count")
                .uid("count")
                .setParallelism(params.getInt("p3", 1));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        // write to dummy sink

        counts.transform("Latency Sink", objectTypeInfo,
                new DummySink<>())
                .uid("dummy-sink")
                .setParallelism(params.getInt("p3", 1));

        // execute program
        env.execute("Stateful WordCount");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static final class Tokenizer implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Long>> out) throws Exception {
            long curTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - curTime < 10) {}

            out.collect(new Tuple2<>(value.f0, Long.valueOf(value.f0)));
        }
    }

    public static final class CountWords extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

        private transient ReducingState<Long> count;

        @Override
        public void open(Configuration parameters) throws Exception {
            ReducingStateDescriptor<Long> descriptor =
                    new ReducingStateDescriptor<Long>(
                            "count", // the state name
                            new Count(),
                            BasicTypeInfo.LONG_TYPE_INFO);

            count = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
//			count.add(value.f1);
            long curTime = System.currentTimeMillis();
//			while (System.currentTimeMillis() - curTime < 1) {}
            out.collect(new Tuple2<>(value.f0, count.get()));
        }

        public static final class Count implements ReduceFunction<Long> {
            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        }
    }
}
