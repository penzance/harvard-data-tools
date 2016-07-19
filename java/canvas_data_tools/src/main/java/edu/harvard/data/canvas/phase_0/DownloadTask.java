package edu.harvard.data.canvas.phase_0;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.ArgumentError;
import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.DataArtifact;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.canvas.data_api.DataFile;

public class DownloadTask implements Callable<Void> {
  private static final Logger log = LogManager.getLogger();

  private final ApiClient api;
  private final String dumpId;
  private final int index;
  private final String tableName;
  private final String expectedFileName;
  private final File tempDir;
  private final DataConfig config;
  private final AwsUtils aws;

  public DownloadTask(final DataConfig config, final ApiClient api, final String dumpId, final String tableName,
      final String expectedFileName, final File tempDir, final int index) {
    this.config = config;
    this.api = api;
    this.dumpId = dumpId;
    this.tableName = tableName;
    this.expectedFileName = expectedFileName;
    this.tempDir = tempDir;
    this.index = index;
    this.aws = new AwsUtils();
  }

  @Override
  public Void call() throws Exception {
    final DataDump dump = api.getDump(dumpId);
    final DataArtifact artifact = dump.getArtifactsByTable().get(tableName);
    final DataFile dataFile = artifact.getFiles().get(index);
    if (!dataFile.getFilename().equals(expectedFileName)) {
      throw new ArgumentError("Mismatch in file name for refreshed dump. Expected"
          + dataFile.getFilename() + ", got " + expectedFileName);
    }
    final String filename = getArtifactFileName(artifact);
    final File downloadFile = new File(tempDir, filename);
    dataFile.download(downloadFile);
    archiveFile(dump, artifact.getTableName(), downloadFile);

    return null;
  }

  private String getArtifactFileName(final DataArtifact artifact) {
    return artifact.getTableName() + "-" + dumpId + "-" + String.format("%05d", index) + ".gz";
  }

  public void archiveFile(final DataDump dump, final String table, final File downloadFile) {
    final String dirName = String.format("%05d", dump.getSequence());
    final S3ObjectId archiveObj = AwsUtils.key(config.getArchiveLocation(), dirName);

    final S3ObjectId infoObj = AwsUtils.key(archiveObj, table, downloadFile.getName());

    // Move the object to the archive bucket.
    aws.getClient().putObject(infoObj.getBucket(), infoObj.getKey(), downloadFile);
    log.info("Uploaded " + downloadFile + " to " + infoObj);
    downloadFile.delete();
  }

}
