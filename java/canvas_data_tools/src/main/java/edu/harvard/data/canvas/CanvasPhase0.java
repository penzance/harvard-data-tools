package edu.harvard.data.canvas;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.TableInfo;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.cli.ArgumentError;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.CanvasDataSchema;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.canvas.phase_0.DumpManager;
import edu.harvard.data.canvas.phase_0.Phase0PostVerifier;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasPhase0 {

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, VerificationException, ArgumentError {
    final String configPathString = args[0];
    final String dumpId = args[1];
    final int threads = Integer.parseInt(args[2]);
    final CanvasDataConfig config = DataConfig.parseInputFiles(CanvasDataConfig.class,
        configPathString, false);
    final CanvasPhase0 phase0 = new CanvasPhase0(config, dumpId, threads);
    phase0.run();
  }

  private final CanvasDataConfig config;
  private final String dumpId;
  private final AwsUtils aws;
  private final DumpManager manager;
  private final ApiClient api;
  private DataDump dump;
  private CanvasDataSchema schema;
  private DumpInfo info;
  private final int threads;

  public CanvasPhase0(final CanvasDataConfig config, final String dumpId, final int threads) {
    this.config = config;
    this.dumpId = dumpId;
    this.threads = threads;
    this.aws = new AwsUtils();
    this.manager = new DumpManager(config, aws);
    this.api = new ApiClient(config.getCanvasDataHost(), config.getCanvasApiKey(),
        config.getCanvasApiSecret());
  }

  private void run() throws DataConfigurationException, UnexpectedApiResponseException, IOException,
  VerificationException, ArgumentError {
    setup();
    downloadDump();
    checkSchema();
    verify();
  }

  private void setup()
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    DumpInfo.init(config.getDumpInfoDynamoTable());
    TableInfo.init(config.getTableInfoDynamoTable());
    this.dump = api.getDump(dumpId);
    this.schema = (CanvasDataSchema) api.getSchema(dump.getSchemaVersion());
    this.info = new DumpInfo(dump.getDumpId(), dump.getSequence(), dump.getSchemaVersion());
  }

  private void downloadDump() throws IOException, UnexpectedApiResponseException,
  DataConfigurationException, VerificationException, ArgumentError {
    manager.saveDump(api, dump, info);
    final S3ObjectId dumpLocation = manager.finalizeDump(dump, schema);
    info.setBucket(dumpLocation.getBucket());
    info.setKey(dumpLocation.getKey());
    info.setDownloaded(true);
    info.save();
    manager.updateTableInfoTable(dump);
  }

  private void checkSchema() throws VerificationException {
    // TODO: Create dynamo table to track valid schemas.
    if (!schema.getVersion().equals("1.10.2")) {
      throw new VerificationException("Unexpected schema version " + schema.getVersion());
    }
  }

  private void verify() throws VerificationException, IOException {
    ExecutorService exec = null;
    try {
      exec = Executors.newFixedThreadPool(threads);
      final Phase0PostVerifier verifier = new Phase0PostVerifier(dumpId, aws, config, exec);
      verifier.verify();
    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
  }
}


