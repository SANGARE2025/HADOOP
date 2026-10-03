package tn.insat.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Job 2 (bonus) : nombre de clients, total et solde moyen par pays.
 * Usage : hadoop jar churn-mapreduce-1.jar tn.insat.tp1.GeographyStats <input> <output>
 */
public class GeographyStats {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GeographyStats <input> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Path output = new Path(args[1]);
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "geography stats");
        job.setJarByClass(GeographyStats.class);
        job.setMapperClass(GeographyStatsMapper.class);
        job.setCombinerClass(GeographyStatsCombiner.class);
        job.setReducerClass(GeographyStatsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
