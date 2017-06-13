package edu.harvard.data.mediasites;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.Phase0;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.mediasites.MediasitesDataConfig;
import edu.harvard.data.pipeline.InputTableIndex;

public class MediasitesPhase0 extends Phase0 {

  private static final Logger log = LogManager.getLogger();
  private final MediasitesDataConfig config;
  private final String runId;
  private final ExecutorService exec;

  public MediasitesPhase0(final MediasitesDataConfig config, final String runId,
      final ExecutorService exec) {
    this.config = config;
    this.runId = runId;
    this.exec = exec;
  }

  @Override
  protected ReturnStatus run() throws IOException, InterruptedException, ExecutionException {
    log.info("Doing whatever Mediasites needs to do...");
    final AwsUtils aws = new AwsUtils();
    final InputTableIndex dataIndex = new InputTableIndex();
    dataIndex.setSchemaVersion("1.0");
    aws.writeJson(config.getIndexFileS3Location(runId), dataIndex);
    return ReturnStatus.OK;
  }
}
