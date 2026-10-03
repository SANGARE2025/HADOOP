package tn.insat.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Locale;

/** Somme finale par pays, ecrite avec 2 decimales (evite la notation 3.11E8). */
public class BalanceSumReducer
        extends Reducer<Text, DoubleWritable, Text, Text> {

    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable v : values) {
            sum += v.get();
        }
        result.set(String.format(Locale.US, "%.2f", sum));
        context.write(key, result);
    }
}
