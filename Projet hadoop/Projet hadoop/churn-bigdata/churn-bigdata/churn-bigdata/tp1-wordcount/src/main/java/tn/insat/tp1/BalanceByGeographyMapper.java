package tn.insat.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Colonnes du CSV (index) :
 * 0 RowNumber, 1 CustomerId, 2 Surname, 3 CreditScore, 4 Geography,
 * 5 Gender, 6 Age, 7 Tenure, 8 Balance, 9 NumOfProducts
 *
 * Entree : une ligne du fichier. Sortie : (Geography, Balance).
 */
public class BalanceByGeographyMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static final int GEOGRAPHY = 4;
    private static final int BALANCE = 8;
    private static final int NB_COLUMNS = 10;

    private final Text country = new Text();
    private final DoubleWritable balance = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // On ignore les lignes vides et la ligne d'en-tete
        if (line.isEmpty() || line.startsWith("RowNumber")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < NB_COLUMNS) {
            context.getCounter("Churn", "MALFORMED_LINES").increment(1);
            return;
        }

        try {
            country.set(fields[GEOGRAPHY].trim());
            balance.set(Double.parseDouble(fields[BALANCE].trim()));
            context.write(country, balance);
            context.getCounter("Churn", "VALID_LINES").increment(1);
        } catch (NumberFormatException e) {
            context.getCounter("Churn", "MALFORMED_LINES").increment(1);
        }
    }
}
