package edu.harvard.data.matterhorn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.pipeline.InputTableIndex;

public class MatterhornPhase0 {

  private static final Logger log = LogManager.getLogger();
  private final MatterhornDataConfig config;

  public static void main(final String[] args)
      throws IOException, DataConfigurationException, InterruptedException, ExecutionException {
    final String configPathString = args[0];
    final String runId = args[1];
    final String datasetId = args[2];
    final int threads = Integer.parseInt(args[3]);
    final MatterhornDataConfig config = DataConfig.parseInputFiles(MatterhornDataConfig.class,
        configPathString, false);
    final MatterhornPhase0 phase0 = new MatterhornPhase0(config);
    phase0.run(datasetId, runId, threads);
  }

  public MatterhornPhase0(final MatterhornDataConfig config) {
    this.config = config;
  }

  private void run(final String datasetId, final String runId, final int threads)
      throws IOException, InterruptedException, ExecutionException {
    log.info("Parsing files");
    final AwsUtils aws = new AwsUtils();
    final InputTableIndex dataIndex = new InputTableIndex();
    ExecutorService exec = null;
    try {
      exec = Executors.newFixedThreadPool(threads);
      final List<Future<InputTableIndex>> jobs = new ArrayList<Future<InputTableIndex>>();
      for (final S3ObjectSummary obj : aws.listKeys(config.getDropboxBucket())) {
        if (obj.getKey().endsWith("actions.2015-09-30.jsonl.gz")) {
          final S3ObjectId outputLocation = AwsUtils.key(config.getS3WorkingLocation(runId));
          final MatterhornSingleFileParser parser = new MatterhornSingleFileParser(obj, outputLocation, config);
          jobs.add(exec.submit(parser));
          log.info("Queuing file " + obj.getBucketName() + "/" + obj.getKey());
        }
      }
      for (final Future<InputTableIndex> job : jobs) {
        dataIndex.addAll(job.get());
      }
    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
    dataIndex.setSchemaVersion("1.0");
    for (final String table : dataIndex.getTableNames()) {
      dataIndex.setPartial(table, true);
    }
    aws.writeJson(config.getIndexFileS3Location(runId), dataIndex);
  }
}

class MatterhornSingleFileParser implements Callable<InputTableIndex> {
  private static final Logger log = LogManager.getLogger();

  private final S3ObjectSummary inputObj;
  private final MatterhornDataConfig config;
  private final S3ObjectId outputLocation;
  private final AwsUtils aws;

  public MatterhornSingleFileParser(final S3ObjectSummary inputObj, final S3ObjectId outputLocation,
      final MatterhornDataConfig config) {
    this.inputObj = inputObj;
    this.outputLocation = outputLocation;
    this.config = config;
    this.aws = new AwsUtils();
  }

  @Override
  public InputTableIndex call() throws Exception {
    log.info("Parsing file " + inputObj.getBucketName() + "/" + inputObj.getKey());
    final InputParser parser = new InputParser(config, aws, AwsUtils.key(inputObj), outputLocation);
    return parser.parseFile();
  }

}
