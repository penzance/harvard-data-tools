package edu.harvard.data.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.harvard.data.client.AwsUtils;
import edu.harvard.data.hadoop.phase1.RequestJob;
import edu.harvard.data.hadoop.phase1.RequestPerFileJob;
import edu.harvard.data.hadoop.phase1.RequestPerPageJob;

public class CanvasDataHadoop {

  public static void main(final String[] args)
      throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    if (args.length != 3) {
      System.err.println("Usage: phase /dump/directory /output/directory");
      System.exit(1);
    }

    final AwsUtils aws = new AwsUtils();
    final Configuration conf = new Configuration();
    final URI hdfsService = new URI("hdfs///");
    final Integer phase = Integer.parseInt(args[0]);
    final String inputDir = args[1];
    final String outputDir = args[2];

    final List<Job> jobs = new ArrayList<Job>();
    switch(phase) {
    case 1:
      jobs.add(new RequestJob(conf, aws, hdfsService, inputDir, outputDir).getJob());
      jobs.add(new RequestPerFileJob(conf, aws, hdfsService, inputDir, outputDir).getJob());
      jobs.add(new RequestPerPageJob(conf, aws, hdfsService, inputDir, outputDir).getJob());
      break;
    case 2:
      break;
    default:
      throw new RuntimeException("Invalid phase: " + phase);
    }

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
