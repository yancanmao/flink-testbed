import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

class Solution {

    public static void main(String[] args) {
        try (
                BufferedReader reader = new BufferedReader(new FileReader("/home/samza/workspace/flink-extended/build-target/log/tmp"));
        ) {
            String str = null;

            int checkpointId = 0;

            Date start = null;
            Date receiveSnapshot = null;
            Date end;

            long redistributeRangeMin = Integer.MAX_VALUE, redistributeRangeMax = Integer.MIN_VALUE;
            long snapshotRangeMin = Integer.MAX_VALUE, snapshotRangeMax = Integer.MIN_VALUE;

            int highSnapshotCnt = 0;

            while ((str = reader.readLine()) != null) {

                if (str.contains("Make rescalepoint with checkpointId")) {
                    checkpointId = Integer.parseInt(str.substring(str.lastIndexOf("=") + 1));
                    start = getTime(str);
                } else if (str.contains("start to assign states")) {
                    receiveSnapshot = getTime(str);

                    if (start != null) {
                        System.out.printf("%d  snapshots use \t\t %d\n", checkpointId, receiveSnapshot.getTime() - start.getTime());

                        if (receiveSnapshot.getTime() - start.getTime() > 1000) {
                            highSnapshotCnt++;
                        }
                        snapshotRangeMin = Math.min(snapshotRangeMin, receiveSnapshot.getTime() - start.getTime());
                        snapshotRangeMax = Math.max(snapshotRangeMax, receiveSnapshot.getTime() - start.getTime());
                    }
                } else if (str.contains("onChangeImplemented triggered for jobVertex")) {
                    end = getTime(str);

                    if (receiveSnapshot != null) {
                        System.out.printf("%d  redistribute use \t\t %d\n", checkpointId, end.getTime() - receiveSnapshot.getTime());

                        redistributeRangeMin = Math.min(redistributeRangeMin, end.getTime() - receiveSnapshot.getTime());
                        redistributeRangeMax = Math.max(redistributeRangeMax, end.getTime() - receiveSnapshot.getTime());
                    }
                }
            }


            System.out.println("snapshot range: " + snapshotRangeMin + " - " + snapshotRangeMax);
            System.out.println("redistribute range: " + redistributeRangeMin + " - " + redistributeRangeMax);
            System.out.println("high snapshot count: " + highSnapshotCnt);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Date getTime(String str) throws ParseException {
        String time = str.substring(0, str.indexOf("INFO"));
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").parse(time);
    }

}
