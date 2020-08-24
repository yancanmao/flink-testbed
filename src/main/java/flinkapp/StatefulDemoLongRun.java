package flinkapp;

import Nexmark.sinks.DummySink;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class StatefulDemoLongRun {

    private static final int MAX = 1000000 * 10;
//    private static final int MAX = 1000;
    private static final int NUM_LETTERS = 26;

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<Tuple2<String, String>> source = env.addSource(new MySource(
                params.getInt("runtime", 10),
                params.getInt("nTuples", 10000),
                params.getInt("nKeys", 1000)
        ));
        DataStream<String> counts = source
            .keyBy(0)
            .map(new MyStatefulMap())
            .disableChaining()
//            .filter(input -> {
//                return Integer.parseInt(input.split(" ")[1]) >= MAX;
//            })
            .name("Splitter FlatMap")
            .uid("flatmap")
            .setParallelism(params.getInt("p2", 3));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        counts.transform("Sink", objectTypeInfo,
                new DummySink<>())
                .uid("dummy-sink")
                .setParallelism(params.getInt("p3", 1));

        env.execute();
//        System.out.println(env.getExecutionPlan());
    }

    private static class MyStatefulMap extends RichMapFunction<Tuple2<String, String>, String> {

        private transient MapState<String, Long> countMap;

        private int count = 0;

        private static Boolean isErrorHappened() throws IOException {
            Scanner scanner = new Scanner(new File("/home/myc/workspace/flink-related/flink-testbed/src/main/resources/err.txt"));
            String line = scanner.nextLine();
            if(line.equals("1")) {
                return true;
            } else {
                // modify and return false
                System.out.println(line);
                try {
                    FileWriter fileWriter = new FileWriter(new File(
                            "/home/myc/workspace/flink-related/flink-testbed/src/main/resources/err.txt"), false);
                    fileWriter.write("1");
                    fileWriter.close();
                } catch (IOException e) {
                    System.out.println("An error occurred.");
                    e.printStackTrace();
                }
                return false;
            }
        }

        @Override
        public String map(Tuple2<String, String> input) throws Exception {
            long start = System.nanoTime();
            while(System.nanoTime() - start < 10000) {}

            String s = input.f0;

            Long cur = countMap.get(s);
            cur = (cur == null) ? 1 : cur + 1;
            countMap.put(s, cur);

            count++;

//            // throw an exception to make task fails
            if (count == 2000 && !isErrorHappened()) {
                int err = count / 0;
            }

//            System.out.println("counted: " + s + " : " + cur + " counter: " + count);

            return String.format("%s %d", s, cur);
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, Long> descriptor =
                    new MapStateDescriptor<>("word-count", String.class, Long.class);

            countMap = getRuntimeContext().getMapState(descriptor);
        }
    }

    private static class MySource implements SourceFunction<Tuple2<String, String>>, CheckpointedFunction {

        private int count = 0;
        private volatile boolean isRunning = true;

        private transient ListState<Integer> checkpointedCount;

        private int runtime;
        private int nTuples;
        private int nKeys;
        private int rate;
        private Map<String, Integer> keyCount = new HashMap<>();

        MySource(int runtime, int nTuples, int nKeys) {
            this.runtime = runtime;
            this.nTuples = nTuples;
            this.nKeys = nKeys;
            this.rate = nTuples / runtime;
            System.out.println("runtime: " + runtime
                    + " nTuples: " + nTuples
                    + " nKeys: " + nKeys
                    + " rate: " + rate);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            this.checkpointedCount.clear();
            this.checkpointedCount.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.checkpointedCount = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("checkpointedCount", Integer.class));

            if (context.isRestored()) {
                for (Integer count : this.checkpointedCount.get()) {
                    this.count = count;
                }
            }
        }

        @Override
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
            // warm up
            Thread.sleep(10000);

            while (isRunning && count < nTuples) {
                if (count % rate  == 0) {
                    Thread.sleep(1000);
                }
                synchronized (ctx.getCheckpointLock()) {
                    String key = getChar(count);
                    int curCount = keyCount.getOrDefault(key, 0)+1;
                    keyCount.put(key, curCount);
//                    System.out.println("sent: " + key + " : " + curCount + " total: " + count);
                    ctx.collect(Tuple2.of(key, key));

                    count++;
                }
            }
        }

//        private String getChar(int cur) {
//            return "A" + (cur % nKeys);
//        }
        private String getChar(int cur) {
            return "A" + cur;
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
