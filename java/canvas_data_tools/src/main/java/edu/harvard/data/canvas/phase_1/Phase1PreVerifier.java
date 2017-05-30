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

/**
 * Preverification sets up the necessary data that will be used by the
 * postverifier to ensure that identify mapping was performed correctly. It does
 * the following things:
 *
 * - Find a set of interesting users (as defined by VerificationPeople). This is
 * a subset of the users in the system chosen for some set of characteristics
 * such as account creation date, activity level and so on. It is hoped that the
 * interesting people will cover a diverse group and should flag any errors in
 * identity mapping.
 *
 * - Fetch the current identity map information for the interesting people
 * directly from Redshift. This will hopefully be redundant, since we've already
 * unloaded the identity map as part of the setup for this phase, but by going
 * directly to the source we may catch any errors that could spring up during
 * the unload operation.
 *
 * - Write that identity information out to the HDFS verification directory,
 * where it can be used by the verification Hadoop jobs.
 *
 * - Kick off the PreVerify Hadoop job defined in the PreVerifyRequestsJob
 * class.
 *
 * The PreVerify job consists only of a mapper, which runs through all the
 * request records and outputs a two-column file that maps from request ID to
 * Canvas Data User ID.
 */
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
    this.outputDir = config.getVerifyHdfsDir(1);
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
    final String interestingIdFile = outputDir + "/ ";
    log.info("Interesting ID file: " + interestingIdFile);
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
