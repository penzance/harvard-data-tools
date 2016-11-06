package edu.harvard.data.canvas;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.ArgumentError;
import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DownloadAndVerify;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.InputTableIndex;
import edu.harvard.data.TableInfo;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.CanvasDataSchema;
import edu.harvard.data.canvas.data_api.DataArtifact;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.canvas.phase_0.DumpManager;
import edu.harvard.data.canvas.phase_0.Phase0PostVerifier;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasDownloadAndVerify extends DownloadAndVerify {

  private final CanvasDataConfig config;
  private String dumpId;
  private String tableName;
  private final AwsUtils aws;
  private final DumpManager manager;
  private final ApiClient api;
  private CanvasDataSchema schema;
  private DumpInfo info;
  private final String runId;
  private final ExecutorService exec;

  public CanvasDownloadAndVerify(final CanvasDataConfig config, final String runId,
      final String datasetId, final ExecutorService exec) throws DataConfigurationException {
    this.config = config;
    this.runId = runId;
    this.exec = exec;
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

  @Override
  protected InputTableIndex run() throws DataConfigurationException, UnexpectedApiResponseException,
  IOException, VerificationException, ArgumentError {
    DumpInfo.init(config.getDumpInfoDynamoTable());
    TableInfo.init(config.getTableInfoDynamoTable());
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
    return index;
  }

  private InputTableIndex getTableHistory()
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    // Calculate the full data set for a single table
    final InputTableIndex index = getFilesForTable();
    index.setPartial(tableName, false);
    index.setSchemaVersion(api.getLatestSchema().getVersion());
    return index;
  }

  private InputTableIndex getFullDump() throws DataConfigurationException,
  UnexpectedApiResponseException, IOException, VerificationException, ArgumentError {
    final DataDump dump = downloadAndVerify();
    final InputTableIndex index = new InputTableIndex();
    index.addAll(manager.getDumpIndex(info.getS3Location()));
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
    final InputTableIndex dataIndex = manager.getDumpIndex(info.getS3Location());
    if (!dataIndex.containsTable(tableName)) {
      throw new VerificationException(
          "Dump " + dump.getSequence() + " does not contain table " + tableName);
    }
    index.addTable(tableName, dataIndex);
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
      downloadDump(dump, exec);
      checkSchema();
      verifyDump(exec);
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

  private InputTableIndex getFilesForTable()
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    final InputTableIndex files = new InputTableIndex();
    final TableInfo tableInfo = TableInfo.find(tableName);
    final long lastComplete = tableInfo.getLastCompleteDumpSequence();
    final List<DumpInfo> dumps = DumpInfo.getAllDumpsSince(lastComplete);
    for (final DumpInfo d : dumps) {
      final InputTableIndex dataIndex = manager.getDumpIndex(d.getS3Location());
      files.addTable(tableName, dataIndex);
    }
    return files;
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

  private void downloadDump(final DataDump dump, final ExecutorService exec)
      throws IOException, UnexpectedApiResponseException, DataConfigurationException,
      VerificationException, ArgumentError {
    info.setDownloadStart(new Date());
    manager.saveDump(api, dump, exec);
    info.setDownloadEnd(new Date());
    final S3ObjectId dumpLocation = manager.finalizeDump(dump, schema);
    info.setBucket(dumpLocation.getBucket());
    info.setKey(dumpLocation.getKey());
    info.setDownloaded(true);
    info.save();
    manager.updateTableInfoTable(dump);
  }

  private void checkSchema() throws VerificationException {
    final Set<String> validSchemas = new HashSet<String>();
    validSchemas.add("1.13.1");
    validSchemas.add("1.13.0");
    validSchemas.add("1.12.1");
    validSchemas.add("1.12.0");
    validSchemas.add("1.11.2");
    validSchemas.add("1.11.1");
    validSchemas.add("1.10.3");
    validSchemas.add("1.10.2");
    validSchemas.add("1.10.1");
    // XXX: Create dynamo table to track valid schemas.
    if (!validSchemas.contains(schema.getVersion())) {
      throw new VerificationException("Unexpected schema version " + schema.getVersion());
    }
  }

  private void verifyDump(final ExecutorService exec) throws VerificationException, IOException {
    final Phase0PostVerifier verifier = new Phase0PostVerifier(dumpId, aws, config, exec);
    verifier.verify();
  }
}
