package tn.insat.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** Pre-agregation locale (cote mapper) : reduit le volume transfere au shuffle. */
public class BalanceSumCombiner
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final DoubleWritable partial = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable v : values) {
            sum += v.get();
        }
        partial.set(sum);
        context.write(key, partial);
    }
}
