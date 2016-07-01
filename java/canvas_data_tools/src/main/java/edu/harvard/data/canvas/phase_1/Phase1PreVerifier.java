package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.HadoopJob;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.leases.LeaseRenewalException;
import edu.harvard.data.leases.LeaseRenewalThread;

public class Phase1PreVerifier {
  private static final Logger log = LogManager.getLogger();
  private final String inputDir;
  private final String outputDir;
  private final Configuration hadoopConfig;
  private final TableFormat format;
  private final CanvasDataConfig config;
  private final URI hdfsService;

  public static void main(final String[] args)
      throws IOException, DataConfigurationException, VerificationException, LeaseRenewalException {
    final String configPathString = args[0];
    final String runId = args[1];
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class,
        configPathString, true);
    final LeaseRenewalThread leaseThread = LeaseRenewalThread.setup(config.getLeaseDynamoTable(),
        config.getIdentityLease(), runId, config.getIdentityLeaseLengthSeconds());
    new Phase1PreVerifier(config).verify();
    leaseThread.checkLease();
  }

  public Phase1PreVerifier(final CanvasDataConfig config) throws DataConfigurationException {
    this.config = config;
    this.inputDir = config.getHdfsDir(0);
    this.outputDir = config.getVerifyHdfsDir(0);
    this.hadoopConfig = new Configuration();
    this.format = new FormatLibrary().getFormat(config.getPipelineFormat());
    try {
      this.hdfsService = new URI("hdfs///");
    } catch (final URISyntaxException e) {
      throw new DataConfigurationException(e);
    }
  }

  public void verify() throws VerificationException, IOException, DataConfigurationException {
    log.info("Running pre-verifier for phase 1");
    log.info("Input directory: " + inputDir);
    log.info("Output directory: " + outputDir);
    final String interestingIdFile = outputDir + "/interesting_canvas_data_ids";
    final VerificationPeople people = new VerificationPeople(config, hadoopConfig, hdfsService,
        inputDir, format);
    try {
      people.storeInterestingIds(interestingIdFile);
    } catch (final SQLException e) {
      throw new IOException(e);
    }

    for (final HadoopJob job : setupJobs()) {
      job.runJob();
      job.addCacheFile(URI.create(interestingIdFile));
    }
  }

  private List<HadoopJob> setupJobs() throws IOException, DataConfigurationException {
    final List<HadoopJob> jobs = new ArrayList<HadoopJob>();
    jobs.add(new PreVerifyRequestsJob(config, 1));
    return jobs;
  }

}
