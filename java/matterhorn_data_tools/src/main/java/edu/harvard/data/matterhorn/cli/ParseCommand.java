package edu.harvard.data.matterhorn.cli;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.VerificationException;
import edu.harvard.data.matterhorn.MatterhornDataConfiguration;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class ParseCommand implements Command {

  private static final Logger log = LogManager.getLogger();

  @Override
  public ReturnStatus execute(final MatterhornDataConfiguration config, final ExecutorService exec)
      throws IOException, UnexpectedApiResponseException, DataConfigurationException,
      VerificationException, ArgumentError {
    log.info("Parsing files");
    final AwsUtils aws = new AwsUtils();
    for (final S3ObjectSummary obj : aws.listKeys(config.getDropboxBucket())) {
      if (obj.getKey().endsWith(".gz")) {
        log.info("Parsing file " + obj.getBucketName() + "/" + obj.getKey());
        new InputParser(config, aws, AwsUtils.key(obj), config.getIncomingBucket()).parseFile();
      }
    }
    return ReturnStatus.OK;
  }

  @Override
  public String getDescription() {
    return "Parse one or more incoming Matterhorn jsonl.gz files";
  }
}
