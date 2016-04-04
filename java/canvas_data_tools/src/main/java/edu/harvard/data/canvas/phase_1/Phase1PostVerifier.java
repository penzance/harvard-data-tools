package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfiguration;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.Verifier;
import edu.harvard.data.canvas.HadoopMultipleJobRunner;

public class Phase1PostVerifier implements Verifier {
  private static final Logger log = LogManager.getLogger();
  private final DataConfiguration config;
  private final Configuration hadoopConfig;
  private final URI hdfsService;
  private final String dataDir;
  private final String verifyDir;

  public Phase1PostVerifier(final DataConfiguration config, final Configuration hadoopConfig, final URI hdfsService,
      final String dataDir, final String verifyDir) {
    this.config = config;
    this.hadoopConfig = hadoopConfig;
    this.hdfsService = hdfsService;
    this.dataDir = dataDir;
    this.verifyDir = verifyDir;
  }

  @Override
  public void verify() throws VerificationException, IOException, DataConfigurationException {
    log.info("Running post-verifier for phase 1");
    log.info("Data directory: " + dataDir);
    log.info("Verify directory: " + verifyDir);
    final HadoopMultipleJobRunner jobRunner = new HadoopMultipleJobRunner(hadoopConfig);
    final List<Job> jobs = setupJobs();
    jobRunner.runParallelJobs(jobs);
  }

  private List<Job> setupJobs() {
    // TODO Auto-generated method stub
    return null;
  }

}
