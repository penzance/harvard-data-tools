package edu.harvard.data.matterhorn;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.matterhorn.cli.HadoopCommand;

public class HadoopMultipleJobRunner {
  private static final Logger log = LogManager.getLogger();

  private final Configuration hadoopConfig;

  public HadoopMultipleJobRunner(final Configuration hadoopConfig) {
    this.hadoopConfig = hadoopConfig;
  }

  public void runParallelJobs(final Collection<Job> jobs)
      throws DataConfigurationException, IOException {
    for (final Job job : jobs) {
      job.setJarByClass(HadoopCommand.class);
      try {
        log.info("Submitted job " + job.getJobName());
        job.submit();
        job.waitForCompletion(true);
      } catch (final ClassNotFoundException e) {
        throw new DataConfigurationException(e);
      } catch (final InterruptedException e) {
        log.error("Job submission interrupted", e);
      }
    }
    for (final Job job : jobs) {
      while (!job.isComplete()) {
        try {
          Thread.sleep(Job.getCompletionPollInterval(hadoopConfig));
        } catch (final InterruptedException e) {
          log.error("Interrupted while waiting for job to complete.", e);
        }
      }
    }
    log.info("All jobs complete");
  }

}
