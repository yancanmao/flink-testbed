/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package Nexmark.queries;

import Nexmark.sinks.DummyLatencyCountingSink;
import Nexmark.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class Query2 {

    private static final Logger logger  = LoggerFactory.getLogger(Query2.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        final int srcRate = params.getInt("srcRate", 100000);
        final int srcCycle = params.getInt("srcCycle", 60);
        final int srcBase = params.getInt("srcBase", 0);
        final int srcWarmUp = params.getInt("srcWarmUp", 100);
        final int srcTupleSize = params.getInt("srcTupleSize", 100);

        DataStream<Bid> bids = env.addSource(new BidSourceFunction(srcRate, srcCycle, srcBase, srcWarmUp*1000, srcTupleSize))
                .setParallelism(params.getInt("p-source", 1))
                .setMaxParallelism(params.getInt("mp2", 64))
                .keyBy(new KeySelector<Bid, Long>() {
                    @Override
                    public Long getKey(Bid bid) throws Exception {
                        return bid.auction;
                    }
                });


        // SELECT Rstream(auction, price)
        // FROM Bid [NOW]
        // WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;

        DataStream<Tuple2<Long, Long>> converted = bids
                .flatMap(new FlatMapFunction<Bid, Tuple2<Long, Long>>() {
                    int count = 0;

                    @Override
                    public void flatMap(Bid bid, Collector<Tuple2<Long, Long>> out) throws Exception {
                        count++;

                        if (count == 200000 && !isErrorHappened()) {
                            int err = count / 0;
                        }

                        long start = System.nanoTime();
                        while(System.nanoTime() - start < 100000) {}

                        if(bid.auction % 1007 == 0 || bid.auction % 1020 == 0 || bid.auction % 2001 == 0 || bid.auction % 2019 == 0 || bid.auction % 2087 == 0) {
                            out.collect(new Tuple2<>(bid.auction, bid.price));
                        }
                    }
                }).setMaxParallelism(params.getInt("mp2", 64))
                .setParallelism(params.getInt("p2", 1))
                .name("flatmap")
                .uid("flatmap");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        converted.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-flatMap", 1));

        // execute program
        env.execute("Nexmark Query2");
    }

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

}