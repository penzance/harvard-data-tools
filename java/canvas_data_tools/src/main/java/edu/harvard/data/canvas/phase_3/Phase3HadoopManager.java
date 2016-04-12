package edu.harvard.data.canvas.phase_3;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfiguration;
import edu.harvard.data.DataConfigurationException;

public class Phase3HadoopManager {
  public static List<Job> getJobs(final AwsUtils aws, final Configuration hadoopConfig,
      final DataConfiguration config, final URI hdfsService, final String inputDir,
      final String outputDir) throws DataConfigurationException, IOException {
    final List<Job> jobs = new ArrayList<Job>();
    jobs.add(new SessionsJob(hadoopConfig, aws, hdfsService, inputDir, outputDir).getJob());
    return jobs;
  }
}
