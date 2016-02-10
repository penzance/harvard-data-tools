package edu.harvard.data.hadoop.requests;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.harvard.data.client.AwsUtils;

public class CanvasDataHadoop {

  public static void main(final String[] args)
      throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    if (args.length != 2) {
      System.err.println("Usage: /dump/directory /output/directory");
      System.exit(1);
    }


    final AwsUtils aws = new AwsUtils();
    final Configuration conf = new Configuration();
    final URI hdfsService = new URI("hdfs///");
    final String inputDir = args[0];
    final String outputDir = args[1];

    final List<Job> jobs = new ArrayList<Job>();
    jobs.add(new RequestJob(conf, aws, hdfsService, inputDir, outputDir).getJob());
    jobs.add(new RequestPerFileJob(conf, aws, hdfsService, inputDir, outputDir).getJob());
    jobs.add(new RequestPerPageJob(conf, aws, hdfsService, inputDir, outputDir).getJob());

    for (final Job job : jobs) {
      job.setJarByClass(CanvasDataHadoop.class);
      job.submit();
    }
    for (final Job job : jobs) {
      while (!job.isComplete()) {
        Thread.sleep(Job.getCompletionPollInterval(conf));
      }
    }
  }

}
