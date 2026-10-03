package com.projetfinal.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Nombre de clients et solde total par pays et par tranche d'age
 * a partir de Customer-Churn-Records.csv.
 * Sortie : "Pays\tTrancheAge\tNbClients\tSoldeTotal".
 *
 * Usage : hadoop jar batch-churn-1.jar com.projetfinal.batch.SoldeParPaysTranche <input> <output>
 */
public class SoldeParPaysTranche {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SoldeParPaysTranche <input> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Path output = new Path(args[1]);
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        Job job = Job.getInstance(conf, "solde par pays et tranche d'age");
        job.setJarByClass(SoldeParPaysTranche.class);
        job.setMapperClass(ChurnMapper.class);
        job.setCombinerClass(ChurnCombiner.class);
        job.setReducerClass(ChurnReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
