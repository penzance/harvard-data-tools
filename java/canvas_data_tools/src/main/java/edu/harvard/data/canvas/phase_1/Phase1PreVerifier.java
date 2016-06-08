package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.HadoopMultipleJobRunner;

public class Phase1PreVerifier {
  private static final Logger log = LogManager.getLogger();
  private final String inputDir;
  private final String outputDir;
  private final Configuration hadoopConfig;
  private final TableFormat format;
  private final CanvasDataConfig config;
  private final URI hdfsService;

  public static void main(final String[] args)
      throws IOException, DataConfigurationException, VerificationException {
    final String configPathString = args[0];
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, configPathString,
        true);
    new Phase1PreVerifier(config).verify();
  }

  public Phase1PreVerifier(final CanvasDataConfig config) throws DataConfigurationException {
    this.config = config;
    this.inputDir = config.getHdfsDir(0);
    this.outputDir = config.getVerifyHdfsDir(0);
    this.hadoopConfig = new Configuration();
    this.format = new FormatLibrary().getFormat(Format.DecompressedCanvasDataFlatFiles);
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

    final HadoopMultipleJobRunner jobRunner = new HadoopMultipleJobRunner(hadoopConfig);
    hadoopConfig.set("format", format.getFormat().toString());
    final List<Job> jobs = setupJobs();
    for (final Job job : jobs) {
      job.addCacheFile(URI.create(interestingIdFile));
    }
    jobRunner.runParallelJobs(jobs);
  }

  private List<Job> setupJobs() throws IOException {
    final List<Job> jobs = new ArrayList<Job>();
    jobs.add(new PreVerifyRequestsJob(hadoopConfig, hdfsService, inputDir, outputDir).getJob());
    return jobs;
  }

}
