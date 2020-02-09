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

        // interval to increase or descrease input rate
        int interval = 50000;
        int interval2 = 100000;
        int count = 0;
        boolean isInc = true;
        boolean inc = true;
        boolean inc2 = true;
        int incTimes = 0;
        long intervalStartTime = System.currentTimeMillis();

        while (running) {

//            System.out.println("++++++++++++sdasd: " + count);
//
//            if (count >= interval && inc) {
//                System.out.println("increase input rate");
//                sentenceRate += 500;
//                inc = false;
//            }
//
//            if (count >= interval2 && inc2) {
//                System.out.println("increase input rate");
//                sentenceRate += 1000;
//                inc2 = false;
//            }

            long emitStartTime = System.currentTimeMillis();
            int cur = 0;
            for (int i = 0; i < sentenceRate; i++) {
                ctx.collect(Tuple2.of(getChar(cur), generator.nextSentence(sentenceSize)));
                cur++;
                count++;
            }
            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                sleep(1000 - emitTime);
            }


            if (System.currentTimeMillis() - intervalStartTime > interval && incTimes < 4) {
                sentenceRate = isInc ? sentenceRate + 500 : sentenceRate - 500;
                intervalStartTime = System.currentTimeMillis();
//                isInc = !isInc;
                incTimes++;
                System.out.println("current input rate is: " +  sentenceRate);
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
