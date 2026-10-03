package tn.insat.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Job 1 (equivalent de "total des ventes par magasin") :
 * total des soldes (Balance) par pays (Geography).
 *
 * Usage : hadoop jar churn-mapreduce-1.jar tn.insat.tp1.BalanceByGeography <input> <output>
 */
public class BalanceByGeography {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: BalanceByGeography <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        // Supprime le repertoire de sortie s'il existe (pratique pour les tests repetes)
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "total balance by geography");
        job.setJarByClass(BalanceByGeography.class);

        job.setMapperClass(BalanceByGeographyMapper.class);
        job.setCombinerClass(BalanceSumCombiner.class);
        job.setReducerClass(BalanceSumReducer.class);

        // Types de sortie du mapper (differents de ceux du reducer)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Types de sortie finale du job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
