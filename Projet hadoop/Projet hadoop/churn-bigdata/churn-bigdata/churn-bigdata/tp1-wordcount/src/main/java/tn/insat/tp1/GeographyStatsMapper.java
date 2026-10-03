package tn.insat.tp1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/** Sortie : (Geography, "1;Balance") -> un client et son solde. */
public class GeographyStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text country = new Text();
    private final Text countAndBalance = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("RowNumber")) {
            return;
        }
        String[] f = line.split(",");
        if (f.length < 10) {
            context.getCounter("Churn", "MALFORMED_LINES").increment(1);
            return;
        }
        try {
            Double.parseDouble(f[8].trim()); // validation
            country.set(f[4].trim());
            countAndBalance.set("1;" + f[8].trim());
            context.write(country, countAndBalance);
        } catch (NumberFormatException e) {
            context.getCounter("Churn", "MALFORMED_LINES").increment(1);
        }
    }
}
