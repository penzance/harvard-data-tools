package edu.harvard.data.matterhorn;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.matterhorn.cli.InputParser;

public class MatterhornPhase0 {

  private static final Logger log = LogManager.getLogger();
  private final MatterhornDataConfig config;

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPathString = args[0];
    final MatterhornDataConfig config = DataConfig.parseInputFiles(MatterhornDataConfig.class,
        configPathString, false);
    final MatterhornPhase0 phase0 = new MatterhornPhase0(config);
    phase0.run();
  }

  public MatterhornPhase0(final MatterhornDataConfig config) {
    this.config = config;
  }

  private void run() throws IOException {
    log.info("Parsing files");
    final AwsUtils aws = new AwsUtils();
    for (final S3ObjectSummary obj : aws.listKeys(config.dropboxBucket)) {
      if (obj.getKey().endsWith(".gz")) {
        log.info("Parsing file " + obj.getBucketName() + "/" + obj.getKey());
        new InputParser(config, aws, AwsUtils.key(obj), config.getS3IncomingLocation()).parseFile();
      }
    }
  }
}
