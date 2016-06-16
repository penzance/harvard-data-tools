package edu.harvard.data.canvas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.TableInfo;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.CanvasDataSchema;
import edu.harvard.data.canvas.data_api.DataArtifact;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.canvas.phase_0.ArgumentError;
import edu.harvard.data.canvas.phase_0.DumpManager;
import edu.harvard.data.canvas.phase_0.Phase0PostVerifier;
import edu.harvard.data.pipeline.InputTableIndex;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasPhase0 {

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, VerificationException, ArgumentError {
    final String configPathString = args[0];
    final String runId = args[1];
    final String datasetId = args[2];
    final int threads = Integer.parseInt(args[3]);
    final CanvasDataConfig config = DataConfig.parseInputFiles(CanvasDataConfig.class,
        configPathString, false);
    DumpInfo.init(config.getDumpInfoDynamoTable());
    TableInfo.init(config.getTableInfoDynamoTable());
    final CanvasPhase0 phase0 = new CanvasPhase0(config, runId, datasetId, threads);
    phase0.run();
  }

  private final CanvasDataConfig config;
  private String dumpId;
  private String tableName;
  private final AwsUtils aws;
  private final DumpManager manager;
  private final ApiClient api;
  private CanvasDataSchema schema;
  private DumpInfo info;
  private final int threads;
  private final String runId;

  public CanvasPhase0(final CanvasDataConfig config, final String runId, final String datasetId,
      final int threads) throws DataConfigurationException {
    this.config = config;
    this.runId = runId;
    this.threads = threads;
    this.aws = new AwsUtils();
    this.manager = new DumpManager(config, aws);
    this.api = new ApiClient(config.getCanvasDataHost(), config.getCanvasApiKey(),
        config.getCanvasApiSecret());
    final String[] datasetParts = datasetId.split(":");
    for (int i = 0; i < datasetParts.length; i += 2) {
      if (datasetParts[i].equals("DUMP")) {
        this.dumpId = datasetParts[i + 1];
      } else if (datasetParts[i].equals("TABLE")) {
        this.tableName = datasetParts[i + 1];
      } else {
        throw new DataConfigurationException("Unknown dataset parameter " + datasetParts[i]);
      }
    }
  }

  private void run() throws DataConfigurationException, UnexpectedApiResponseException, IOException,
  VerificationException, ArgumentError {
    final InputTableIndex index;
    if (dumpId == null && tableName != null) {
      // Build a data set containing the full history of one table
      index = getTableHistory();

    } else if (dumpId != null && tableName == null) {
      // Build a data set containing one complete dump
      index = getFullDump();

    } else if (dumpId != null && tableName != null) {
      // Build a data set containing the files for one table in one dump
      index = getTableForDump();

    } else {
      // Error; we need either dump ID or a table name.
      throw new ArgumentError("Either dump ID or table name must be set");
    }

    // Write the index
    final S3ObjectId indexFile = config.getIndexFileS3Location(runId);
    System.out.println("Saving index to " + AwsUtils.uri(indexFile));
    aws.writeJson(indexFile, index);
  }

  private InputTableIndex getTableHistory()
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    // Calculate the full data set for a single table
    final InputTableIndex index = new InputTableIndex();
    final List<S3ObjectId> directories = getDirectoriesForTable();
    index.addDirectories(tableName, directories);
    index.setPartial(tableName, false);
    index.setSchemaVersion(api.getLatestSchema().getVersion());
    return index;
  }

  private InputTableIndex getFullDump() throws DataConfigurationException,
  UnexpectedApiResponseException, IOException, VerificationException, ArgumentError {
    final DataDump dump = downloadAndVerify();
    final InputTableIndex index = new InputTableIndex();
    index.addTables(manager.getDumpIndex(dump.getSequence()));
    for (final String table : index.getTableNames()) {
      index.setPartial(table, isPartial(table, dump));
    }
    index.setSchemaVersion(dump.getSchemaVersion());
    return index;
  }

  private InputTableIndex getTableForDump() throws DataConfigurationException,
  UnexpectedApiResponseException, IOException, VerificationException, ArgumentError {
    final DataDump dump = downloadAndVerify();
    final InputTableIndex index = new InputTableIndex();
    final Map<String, List<S3ObjectId>> dumpTables = manager.getDumpIndex(dump.getSequence());
    if (!dumpTables.containsKey(tableName)) {
      throw new VerificationException(
          "Dump " + dump.getSequence() + " does not contain table " + tableName);
    }
    index.addDirectories(tableName, dumpTables.get(tableName));
    index.setPartial(tableName, isPartial(tableName, dump));
    index.setSchemaVersion(dump.getSchemaVersion());
    return index;
  }

  private DataDump downloadAndVerify() throws DataConfigurationException,
  UnexpectedApiResponseException, IOException, VerificationException, ArgumentError {
    // Download and verify the dump
    final DataDump dump = setupForDump();
    // Bypass the download and verify step if it's already happened
    if (!info.getVerified()) {
      downloadDump(dump);
      checkSchema();
      verifyDump();
    }
    return dump;
  }

  private boolean isPartial(final String table, final DataDump dump) throws VerificationException {
    for (final DataArtifact artifact : dump.getArtifactsByTable().values()) {
      if (artifact.getTableName().equals(table)) {
        return artifact.isPartial();
      }
    }
    throw new VerificationException("Can't determine whether " + table + " is partial");
  }

  private List<S3ObjectId> getDirectoriesForTable()
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    final List<S3ObjectId> directories = new ArrayList<S3ObjectId>();
    final TableInfo tableInfo = TableInfo.find(tableName);
    final long lastComplete = tableInfo.getLastCompleteDumpSequence();
    final List<DumpInfo> dumps = DumpInfo.getAllDumpsSince(lastComplete);
    for (final DumpInfo d : dumps) {
      final Map<String, List<S3ObjectId>> tables = manager.getDumpIndex(d.getSequence());
      if (tables.containsKey(tableName)) {
        directories.addAll(tables.get(tableName));
      }
    }
    return directories;
  }

  private DataDump setupForDump()
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    final DataDump dump = api.getDump(dumpId);
    this.schema = (CanvasDataSchema) api.getSchema(dump.getSchemaVersion());
    this.info = DumpInfo.find(dumpId);
    if (this.info == null) {
      this.info = new DumpInfo(dump.getDumpId(), dump.getSequence(), dump.getSchemaVersion());
    }
    return dump;
  }

  private void downloadDump(final DataDump dump) throws IOException, UnexpectedApiResponseException,
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
    // XXX: Create dynamo table to track valid schemas.
    if (!schema.getVersion().equals("1.10.3")) {
      throw new VerificationException("Unexpected schema version " + schema.getVersion());
    }
  }

  private void verifyDump() throws VerificationException, IOException {
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
