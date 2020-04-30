package flinkapp.wordcount;

import Nexmark.sinks.DummySink;
import flinkapp.wordcount.sources.RateControlledSourceFunctionKV;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

public class StatefulWordCount {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		env.setStateBackend(new FsStateBackend("file:///home/myc/workspace/flink-related/states"));

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		env.enableCheckpointing(1000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		FlinkKafkaProducer011<String> kafkaProducer = new FlinkKafkaProducer011<String>(
				"localhost:9092", "word_count_output", new SimpleStringSchema());
		kafkaProducer.setWriteTimestampToKafka(true);

		final DataStream<Tuple2<String, String>> text = env.addSource(
				new RateControlledSourceFunctionKV(
						params.getInt("source-rate", 150),
						params.getInt("sentence-size", 100)))
				.uid("sentence-source")
					.setParallelism(params.getInt("p1", 1));

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

//		counts
//			.keyBy(0)
//			.map(new MapFunction<Tuple2<String,Long>, String>() {
//				@Override
//				public String map(Tuple2<String, Long> tuple) {
//					return tuple.f0;
//				}
//			})
//			.name("Projector")
//			.uid("projector")
//				.setParallelism(params.getInt("p4", 1))
//			.addSink(kafkaProducer)
//			.name("Sink")
//			.uid("sink")
//				.setParallelism(params.getInt("p5", 1));

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

	public static final class Tokenizer extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;

        private transient MapState<String, Long> countMap;

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, Long> descriptor =
                    new MapStateDescriptor<>("splitter", String.class, Long.class);

            countMap = getRuntimeContext().getMapState(descriptor);
        }

		@Override
		public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, Long>> out) throws Exception {
//			long curTime = System.currentTimeMillis();
//			while (System.currentTimeMillis() - curTime < 1) {}

			// normalize and split the line
			String[] tokens = value.f1.toLowerCase().split("\\W+");

			Long cur = countMap.get(value.f0);
			cur = (cur == null) ? 1 : cur + 1;
			countMap.put(value.f0, cur);

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1L));
				}
			}
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
