package flinkapp.wordcount.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import static java.lang.Thread.sleep;

public class RateControlledSourceFunctionKV extends RichParallelSourceFunction<Tuple2<String, String>> {

    /** how many sentences to output per second **/
    private int sentenceRate;
//    private final int sentenceRate;

    /** the length of each sentence (in chars) **/
    private final int sentenceSize;

    private final RandomSentenceGenerator generator;

    private volatile boolean running = true;

    private static final int NUM_LETTERS = 128;

    public RateControlledSourceFunctionKV(int rate, int size) {
        sentenceRate = rate;
        generator = new RandomSentenceGenerator();
        sentenceSize = size;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {

        while (running) {

            long emitStartTime = System.currentTimeMillis();
            int cur = 0;
            for (int i = 0; i < sentenceRate/50; i++) {
                ctx.collect(Tuple2.of(getChar(cur), generator.nextSentence(sentenceSize)));
                cur++;
            }
            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000/50) {
                sleep(1000/50 - emitTime);
            }
        }

        ctx.close();
    }

    @Override
    public void cancel() {
        running = false;
    }

    private static String getChar(int cur) {
        return String.valueOf(Math.random()%1024);
    }
}
