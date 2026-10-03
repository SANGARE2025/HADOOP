package com.projetfinal.batch;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** Additionne (nombre, somme) partiels cote mapper. Format conserve : "count;sum". */
public class ChurnCombiner extends Reducer<Text, Text, Text, Text> {

    private final Text out = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long count = 0;
        double sum = 0;
        for (Text v : values) {
            String[] p = v.toString().split(";");
            count += Long.parseLong(p[0]);
            sum += Double.parseDouble(p[1]);
        }
        out.set(count + ";" + sum);
        context.write(key, out);
    }
}
