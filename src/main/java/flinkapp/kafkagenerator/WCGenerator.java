package flinkapp.kafkagenerator;

import Nexmark.sources.Util;
import org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * SSE generaor
 */
public class WCGenerator {

    private final String TOPIC = "words";

    private static KafkaProducer<String, String> producer;

    private long SENTENCE_NUM = 1000000000;
    private int uniformSize = 10000;
    private double mu = 10;
    private double sigma = 1;

    private int count = 0;

    private transient ListState<Integer> checkpointedCount;

    private int runtime;
    private int nTuples;
    //    private int rate;

    int warmUpInterval = 60000;
    int cycle = 300;
    int epoch = 0;
    int base = 40000;
    int rate = 30000;
    int curRate = base + rate;
    private volatile boolean isRunning = true;

    private int nextKey = 0;

    public WCGenerator(int runtime, int nTuples, int nKeys) {
//        this.runtime = runtime;
//        this.nTuples = nTuples;
//        this.nKeys = nKeys;
//        this.rate = nTuples / runtime;
        System.out.println("runtime: " + runtime
                + ", nTuples: " + nTuples
                + ", nKeys: " + nKeys
                + ", rate: " + rate);
        initProducer();
    }

    private void initProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public void generate() throws InterruptedException {

        long emitStartTime;
        long startTs = System.currentTimeMillis();

        while (isRunning && !Thread.interrupted()) {
            emitStartTime = System.currentTimeMillis();
            if (System.currentTimeMillis() - startTs < warmUpInterval) {
                if (count == 20) {
                    // change input rate every 1 second.
                    epoch++;
                    System.out.println("epoch: " + epoch % cycle + " current rate is: " + curRate);
                    count = 0;
                }

                for (int i = 0; i < Integer.valueOf(curRate / 20); i++) {
                    String key = getChar(nextKey);
                    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, key);
                    producer.send(newRecord);
                    nextKey++;
                }
                // Sleep for the rest of timeslice if needed
                Util.pause(emitStartTime);
                count++;
            } else { // after warm up

                if (count == 20) {
                    // change input rate every 1 second.
                    epoch++;
                    curRate = base + Util.changeRateSin(rate, cycle, epoch);
                    System.out.println("epoch: " + epoch % cycle + " current rate is: " + curRate);
                    count = 0;
                }

                for (int i = 0; i < curRate / 20; i++) {
                    String key = getChar(nextKey);

                    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, key);
                    producer.send(newRecord);

                    nextKey++;
                }

                // Sleep for the rest of timeslice if needed
                Util.pause(emitStartTime);
                count++;
            }
        }

        producer.close();
    }

    private String getChar(int cur) {
        int nKeys = 10000;
        return "A" + (cur % nKeys);
    }

    public static void main(String[] args) throws InterruptedException {
        final ParameterTool params = ParameterTool.fromArgs(args);

        new WCGenerator(
                params.getInt("runtime", 10),
                params.getInt("nTuples", 10000),
                params.getInt("nKeys", 1000))
                .generate();
    }
}