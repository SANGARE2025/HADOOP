package tn.insat.tp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Locale;

/** Sortie : Geography <TAB> nombre_clients <TAB> total <TAB> moyenne. */
public class GeographyStatsReducer extends Reducer<Text, Text, Text, Text> {

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
        double avg = count == 0 ? 0 : sum / count;
        out.set(String.format(Locale.US, "%d\t%.2f\t%.2f", count, sum, avg));
        context.write(key, out);
    }
}
