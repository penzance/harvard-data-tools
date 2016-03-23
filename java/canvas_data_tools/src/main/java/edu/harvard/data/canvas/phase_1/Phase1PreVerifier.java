package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.Verifier;
import edu.harvard.data.canvas.HadoopMultipleJobRunner;

public class Phase1PreVerifier implements Verifier {
  private static final Logger log = LogManager.getLogger();
  private final String inputDir;
  private final String outputDir;
  private final URI hdfsService;
  private final Configuration hadoopConfig;

  public Phase1PreVerifier(final Configuration hadoopConfig, final URI hdfsService, final String inputDir, final String outputDir) {
    this.hadoopConfig = hadoopConfig;
    this.hdfsService = hdfsService;
    this.inputDir = inputDir;
    this.outputDir = outputDir;
  }

  @Override
  public void verify() throws VerificationException, IOException, DataConfigurationException {
    log.info("Running pre-verifier for phase 1");
    log.info("Input directory: " + inputDir);
    log.info("Output directory: " + outputDir);
    final HadoopMultipleJobRunner jobRunner = new HadoopMultipleJobRunner(hadoopConfig);
    final List<Job> jobs = setupJobs();
    jobRunner.runParallelJobs(jobs);
  }

  private List<Job> setupJobs() throws IOException {
    final List<Job> jobs = new ArrayList<Job>();
    jobs.add(new PreVerifyRequestsJob(hadoopConfig, hdfsService, inputDir, outputDir).getJob());
    return jobs;
  }

}
