package edu.harvard.canvas_data.aws_data_tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.canvas_data.aws_data_tools.cli.ReturnStatus;
import edu.harvard.data.client.AwsUtils;
import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.canvas.api.CanvasApiClient;
import edu.harvard.data.client.canvas.api.CanvasDataArtifact;
import edu.harvard.data.client.canvas.api.CanvasDataDump;
import edu.harvard.data.client.canvas.api.CanvasDataFile;
import edu.harvard.data.client.canvas.api.CanvasDataSchema;
import edu.harvard.data.client.canvas.api.UnexpectedApiResponseException;

public class DumpManager {

  private static final Logger log = LogManager.getLogger();

  private final DataConfiguration config;
  private final AwsUtils aws;

  public DumpManager(final DataConfiguration config, final AwsUtils aws) {
    this.config = config;
    this.aws = aws;
  }

  public boolean needToSaveDump(final CanvasDataDump dump) throws IOException {
    final DumpInfo info = DumpInfo.find(dump.getDumpId());
    if (dump.getSequence() < 80) {
      log.warn("Dump downloader set to ignore dumps with sequence < 80");
      return false;
    }
    if (info == null) {
      log.info("Dump needs to be saved; no dump info record for " + dump.getDumpId());
      return true;
    }
    if (info.getDownloaded() == null || !info.getDownloaded()) {
      log.info("Dump needs to be saved; previous download did not complete.");
      return true;
    }
    final Date downloadStart = info.getDownloadStart();
    // Re-download any dump that was updated less than an hour before it was
    // downloaded before.
    final Date conservativeStart = new Date(downloadStart.getTime() - (60 * 60 * 1000));
    if (conservativeStart.before(dump.getUpdatedAt())) {
      log.info(
          "Dump needs to be saved; previously downloaded less than an hour after it was last updated.");
      return true;
    }
    log.info("Dump does not need to be saved; already exists at " + info.getBucket() + "/"
        + info.getKey() + ".");
    return false;
  }

  public void saveDump(final CanvasApiClient api, final CanvasDataDump dump, final DumpInfo info)
      throws IOException, UnexpectedApiResponseException, DataConfigurationException, VerificationException {
    info.setDownloadStart(new Date());
    final File directory = getScratchDumpDir(dump);
    final boolean created = directory.mkdirs();
    log.debug("Creating directory " + directory + ": " + created);
    final Map<String, CanvasDataArtifact> artifactsByTable = dump.getArtifactsByTable();
    final List<String> tables = new ArrayList<String>(artifactsByTable.keySet());
    int downloadedFiles = 0;
    for (final String table : tables) {
      int fileIndex = 0;
      final File tableDir = new File(directory, table);
      final CanvasDataArtifact artifact = artifactsByTable.get(table);
      log.info("Dumping " + table + " to " + tableDir);
      final List<CanvasDataFile> files = artifact.getFiles();
      for (int i = 0; i < files.size(); i++) {
        final CanvasDataFile file = files.get(i);
        final CanvasDataDump refreshedDump = api.getDump(dump.getDumpId());
        final CanvasDataArtifact refreshedArtifact = refreshedDump.getArtifactsByTable().get(table);
        final CanvasDataFile refreshedFile = refreshedArtifact.getFiles().get(i);
        if (!refreshedFile.getFilename().equals(file.getFilename())) {
          throw new FatalError(ReturnStatus.API_ERROR,
              "Mismatch in file name for refreshed dump. Expected" + refreshedFile.getFilename()
              + ", got " + file.getFilename());
        }
        final String filename = artifact.getTableName() + "-" + String.format("%05d", fileIndex++)
        + ".gz";
        final File downloadFile = new File(tableDir, filename);
        refreshedFile.download(downloadFile);
        archiveFile(dump, table, downloadFile);
        downloadedFiles++;
      }
    }
    if (downloadedFiles != dump.countFilesToDownload()) {
      throw new VerificationException("Expected to download " + dump.getNumFiles()
      + " files. Actually downloaded " + downloadedFiles);
    }
    info.setDownloadEnd(new Date());
  }

  public void archiveFile(final CanvasDataDump dump, final String table, final File downloadFile) {
    final S3ObjectId archiveObj = getArchiveDumpObj(dump);
    final S3ObjectId infoObj = AwsUtils.key(archiveObj, table, downloadFile.getName());
    aws.getClient().putObject(infoObj.getBucket(), infoObj.getKey(), downloadFile);
    log.info("Uploaded " + downloadFile + " to " + infoObj);
    downloadFile.delete();
  }

  public S3ObjectId finalizeDump(final CanvasDataDump dump, final CanvasDataSchema schema)
      throws IOException {
    final S3ObjectId archiveObj = getArchiveDumpObj(dump);
    aws.writeJson(AwsUtils.key(archiveObj, "schema.json"), schema);
    aws.writeJson(AwsUtils.key(archiveObj, "dump_info.json"), dump);
    return archiveObj;
  }

  public void deleteTemporaryDump(final CanvasDataDump dump) throws IOException {
    final File directory = getScratchDumpDir(dump);
    FileUtils.deleteDirectory(directory);
  }

  private File getScratchDumpDir(final CanvasDataDump dump) {
    final String dirName = String.format("%05d", dump.getSequence());
    return new File(config.getScratchDir(), dirName);
  }

  public S3ObjectId getArchiveDumpObj(final CanvasDataDump dump) {
    final String dirName = String.format("%05d", dump.getSequence());
    return AwsUtils.key(config.getCanvasDataArchiveKey(), dirName);
  }

}
