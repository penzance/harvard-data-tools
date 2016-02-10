package edu.harvard.data.hadoop.requests;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RequestsFileHadoopJob {

  public static void main(final String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: s3://path/to/data/file s3://output/directory");
    }

    final Configuration conf = new Configuration();

    final Job job = Job.getInstance(conf, "requests-files-hadoop");
    job.setJarByClass(RequestsFileHadoopJob.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(RequestFileMapper.class);
    job.setReducerClass(RequestFileReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }

}
