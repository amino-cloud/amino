package com._42six.amino.ingestion_tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Calendar;

public class DateGenerator {

    // for no particular reason: Sat Jun 21 14:40:58 UTC 2003
    private static final long startTimestamp = 1056206458;
    // delta is 100,000 because 24 hrs is 86,400 seconds. we want to hit most days
    // but want different times of day throughout
    private static final int delta = 100000000;
    private static final int dayDelta = 1;
    // 5000 iterations at this delta is around 16 years.
    private static final int count = 5000;

    public static void main(String[] args) throws IOException {
                createFile(String.format("timestamps-%d.txt", count), startTimestamp, count, delta);
    }

    private static void createFile(String fileName, long timestamp, int count, int delta) throws IOException {
        File f = new File("target/" + fileName);
        BufferedWriter out = new BufferedWriter(new FileWriter(f));

        Calendar c = Calendar.getInstance();
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
