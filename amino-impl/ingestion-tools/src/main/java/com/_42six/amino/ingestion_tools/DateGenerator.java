package com._42six.amino.ingestion_tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;

public class DateGenerator {

    // for no particular reason: Sat Jun 21 14:40:58 UTC 2003
    private static final long startTimestamp = 1056206458;
    private static final int dayDelta = 1;
    // 5000 iterations at this delta is around 16 years.
    private static final int count = 5000;

    public static void main(String[] args) throws IOException {
        createFile(String.format("timestamps-%d.txt", count), startTimestamp, count);
    }

    private static void createFile(String fileName, long timestamp, int count) throws IOException {
        final File f = new File("target/" + fileName);
        final BufferedWriter out = new BufferedWriter(new FileWriter(f));
        final Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timestamp*1000);

        for (int i = 1; i <= count; ++i) {
            // store timestamps in ms
            out.write(String.format("%d", c.getTimeInMillis()));

            if (i != count) {
                out.write("\r\n");
            }

            c.add(Calendar.DATE, dayDelta);
        }
        out.close();
    }

}
