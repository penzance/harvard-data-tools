package edu.harvard.data.canvas.phase_1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopUtilities;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.HadoopMultipleJobRunner;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.io.HdfsTableReader;

public class Phase1PostVerifier {
  private static final Logger log = LogManager.getLogger();
  private final Configuration hadoopConfig;
  private final URI hdfsService;
  private final String inputDir;
  private final String outputDir;
  private final String verifyDir;
  private final TableFormat format;
  private final HadoopUtilities hadoopUtils;

  public static void main(final String[] args)
      throws IOException, DataConfigurationException, VerificationException {
    final String configPathString = args[0];
    final CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, configPathString,
        true);
    new Phase1PreVerifier(config).verify();
  }

  public Phase1PostVerifier(final CanvasDataConfig config) throws DataConfigurationException {
    this.inputDir = config.getHdfsDir(0);
    this.outputDir = config.getHdfsDir(1);
    this.verifyDir = config.getVerifyHdfsDir(0);
    this.hadoopConfig = new Configuration();
    this.hadoopUtils = new HadoopUtilities();
    this.format = new FormatLibrary().getFormat(Format.DecompressedCanvasDataFlatFiles);
    try {
      this.hdfsService = new URI("hdfs///");
    } catch (final URISyntaxException e) {
      throw new DataConfigurationException(e);
    }
  }

  public void verify() throws VerificationException, IOException, DataConfigurationException {
    log.info("Running post-verifier for phase 1");
    log.info("Input directory: " + inputDir);
    log.info("Output directory: " + outputDir);
    log.info("Verify directory: " + verifyDir);

    new PostVerifyIdentityMap(hadoopConfig, hdfsService, inputDir + "/identity_map",
        outputDir + "/identity_map", format).verify();
    updateInterestingTables();

    hadoopConfig.set("format", format.getFormat().toString());
    final HadoopMultipleJobRunner jobRunner = new HadoopMultipleJobRunner(hadoopConfig);
    final List<Job> jobs = setupJobs();
    jobRunner.runParallelJobs(jobs);
  }

  private List<Job> setupJobs() throws IOException {
    final AwsUtils aws = new AwsUtils();
    final List<Job> jobs = new ArrayList<Job>();
    jobs.add(
        new PostVerifyRequestsJob(hadoopConfig, aws, hdfsService, outputDir, verifyDir).getJob());
    return jobs;
  }

  private void updateInterestingTables() throws IOException {
    final FileSystem fs = FileSystem.get(hdfsService, hadoopConfig);
    final Map<Long, IdentityMap> identities = new HashMap<Long, IdentityMap>();
    for (final Path path : hadoopUtils.listFiles(hdfsService, outputDir + "/identity_map")) {
      try (HdfsTableReader<IdentityMap> in = new HdfsTableReader<IdentityMap>(IdentityMap.class,
          format, fs, path)) {
        for (final IdentityMap id : in) {
          identities.put((Long) id.get(IdentifierType.CanvasDataID), id);
        }
      }
    }

    for (final Path path : hadoopUtils.listFiles(hdfsService, verifyDir + "/requests")) {
      try (FSDataInputStream fsin = fs.open(path);
          BufferedReader in = new BufferedReader(new InputStreamReader(fsin));
          FSDataOutputStream out = fs
              .create(new Path(verifyDir + "/updated/requests/" + hadoopUtils.getFileName(path)))) {
        String line = in.readLine();
        while (line != null) {
          final String[] parts = line.split("\t");
          final Long oldId = Long.parseLong(parts[1]);
          if (!identities.containsKey(oldId)) {
            throw new RuntimeException(
                "Verification error: Canvas Data ID " + oldId + " missing from identity map");
          }
          final String newId = (String) identities.get(oldId).get(IdentifierType.ResearchUUID);
          out.writeBytes(parts[0] + "\t" + newId + "\n");
          line = in.readLine();
        }
      }
    }
  }
}
