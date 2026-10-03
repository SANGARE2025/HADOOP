package com.projetfinal.batch;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Ligne attendue (CSV, virgule) :
 * RowNumber,CustomerId,Surname,CreditScore,Geography,Gender,Age,Tenure,Balance,NumOfProducts
 *
 * Sortie : cle "Geography\tTrancheAge" -> valeur "1;Balance" (un client et son solde).
 */
public class ChurnMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    static String ageBand(int age) {
        if (age < 30) return "<30";
        if (age < 40) return "30-39";
        if (age < 50) return "40-49";
        if (age < 60) return "50-59";
        return "60+";
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("RowNumber")) {
            return; // en-tete / ligne vide
        }
        String[] f = line.split(",");
        if (f.length < 10) {
            context.getCounter("Churn", "MALFORMED_LINES").increment(1);
            return;
        }
        try {
            int age = Integer.parseInt(f[6].trim());
            Double.parseDouble(f[8].trim()); // validation du solde
            outKey.set(f[4].trim() + "\t" + ageBand(age));
            outValue.set("1;" + f[8].trim());
            context.write(outKey, outValue);
        } catch (NumberFormatException e) {
            context.getCounter("Churn", "MALFORMED_LINES").increment(1);
        }
    }
}
