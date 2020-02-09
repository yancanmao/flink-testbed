package flinkapp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

import static java.lang.Thread.sleep;

public class WordCount {

    private static final String INPUT_STREAM_ID = "wc_input";
    private static final String OUTPUT_STREAM_ID = "wc_output";
    private static final String KAFKA_BROKERS = "localhost:9092";

    public static void main(String[] args) throws Exception {


        final SourceFunction<String> source;

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS);

        FlinkKafkaConsumer011<String> inputConsumer = new FlinkKafkaConsumer011<>(INPUT_STREAM_ID, new SimpleStringSchema(), kafkaProps);

        inputConsumer.setStartFromLatest();
        inputConsumer.setCommitOffsetsOnCheckpoints(false);

        source = inputConsumer;

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        see.enableCheckpointing(2000L);


        DataStream<String> words = see.addSource(source);

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                words.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0).sum(1);

        // emit result
        System.out.println("Printing result to stdout. Use --output to specify output path.");
        counts.print();

        counts
                .map(new MapFunction<Tuple2<String,Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> tuple) {
                        return tuple.toString();
                    }
                })
                .addSink(new FlinkKafkaProducer011<>(KAFKA_BROKERS, OUTPUT_STREAM_ID, new SimpleStringSchema()));

        // execute program
        see.execute("Streaming WordCount");
        long processingStart = System.nanoTime();
        System.out.println(processingStart);
        sleep(1000);
        System.out.println(System.nanoTime() - processingStart);
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
