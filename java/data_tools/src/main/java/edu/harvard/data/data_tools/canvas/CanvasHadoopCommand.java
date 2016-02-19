package edu.harvard.data.data_tools.canvas;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.kohsuke.args4j.Argument;
import org.mortbay.log.Log;

import edu.harvard.data.client.AwsUtils;
import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.schema.UnexpectedApiResponseException;
import edu.harvard.data.data_tools.Command;
import edu.harvard.data.data_tools.ReturnStatus;
import edu.harvard.data.data_tools.VerificationException;
import edu.harvard.data.data_tools.canvas.phase1.RequestJob;
import edu.harvard.data.data_tools.canvas.phase1.RequestPerFileJob;
import edu.harvard.data.data_tools.canvas.phase1.RequestPerPageJob;

public class CanvasHadoopCommand implements Command {

  @Argument(index = 0, usage = "Hadoop process phase.", metaVar = "0", required = true)
  public int phase;

  @Argument(index = 1, usage = "Input directory on HDFS.", metaVar = "/dump/directory", required = true)
  public String inputDir;

  @Argument(index = 2, usage = "Output directory on HDFS.", metaVar = "/output/directory", required = true)
  public String outputDir;

  @Override
  public ReturnStatus execute(final DataConfiguration config) throws IOException,
  UnexpectedApiResponseException, DataConfigurationException, VerificationException {
    final AwsUtils aws = new AwsUtils();
    final Configuration conf = new Configuration();
    final URI hdfsService;
    try {
      hdfsService = new URI("hdfs///");
    } catch (final URISyntaxException e) {
      throw new DataConfigurationException(e);
    }

    final List<Job> jobs = new ArrayList<Job>();
    switch (phase) {
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
      job.setJarByClass(CanvasHadoopCommand.class);
      try {
        job.submit();
      } catch (final ClassNotFoundException e) {
        throw new DataConfigurationException(e);
      } catch (final InterruptedException e) {
        Log.info("Job submission interrupted", e);
      }
    }
    for (final Job job : jobs) {
      while (!job.isComplete()) {
        try {
          Thread.sleep(Job.getCompletionPollInterval(conf));
        } catch (final InterruptedException e) {
          Log.info("Interrupted while waiting for job to complete.", e);
        }
      }
    }
    return ReturnStatus.OK;
  }

  @Override
  public String getDescription() {
    return "Create and run the Canvas hadoop jobs for a single phase";
  }

}
