package edu.harvard.data.canvas.phase_0;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.TableInfo;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.CanvasDataSchema;
import edu.harvard.data.canvas.data_api.DataArtifact;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.canvas.data_api.DataFile;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class DumpManager {

  private static final Logger log = LogManager.getLogger();

  private final CanvasDataConfig config;
  private final AwsUtils aws;

  public DumpManager(final CanvasDataConfig config, final AwsUtils aws) {
    this.config = config;
    this.aws = aws;
  }

  // XXX This currently runs sequentially. The difficulty with parallelizing it
  // is that the URLs that we get through the dump expire (in ~15 minutes), so
  // we need to refresh the dump between when we set up a concurrent task and
  // when it runs. In addition, the file names that Instructure send are not
  // guaranteed to be unique, so we need to be smart in making sure that we
  // download the correct file.
  public void saveDump(final ApiClient api, final DataDump dump, final DumpInfo info,
      final ExecutorService exec) throws IOException, UnexpectedApiResponseException,
  DataConfigurationException, VerificationException, ArgumentError {
    info.setDownloadStart(new Date());
    final File directory = getScratchDumpDir(dump);
    final boolean created = directory.mkdirs();
    log.debug("Creating directory " + directory + ": " + created);
    final Map<String, DataArtifact> artifactsByTable = dump.getArtifactsByTable();
    final List<String> tables = new ArrayList<String>(artifactsByTable.keySet());
    int downloadedFiles = 0;
    for (final String table : tables) {
      int fileIndex = 0;
      final File tableDir = new File(directory, table);
      final DataArtifact artifact = artifactsByTable.get(table);
      log.info("Dumping " + table + " to " + tableDir);
      final List<DataFile> files = artifact.getFiles();
      for (int i = 0; i < files.size(); i++) {
        final DataFile file = files.get(i);
        final DataDump refreshedDump = api.getDump(dump.getDumpId());
        final DataArtifact refreshedArtifact = refreshedDump.getArtifactsByTable().get(table);
        final DataFile refreshedFile = refreshedArtifact.getFiles().get(i);
        if (!refreshedFile.getFilename().equals(file.getFilename())) {
          throw new ArgumentError("Mismatch in file name for refreshed dump. Expected"
              + refreshedFile.getFilename() + ", got " + file.getFilename());
        }
        final String filename = getArtifactFileName(refreshedDump.getDumpId(), artifact,
            fileIndex++);
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

  private String getArtifactFileName(final String dumpId, final DataArtifact artifact,
      final int fileIndex) {
    return artifact.getTableName() + "-" + dumpId + "-" + String.format("%05d", fileIndex) + ".gz";
  }

  public void archiveFile(final DataDump dump, final String table, final File downloadFile) {
    final S3ObjectId archiveObj = getArchiveDumpObj(dump.getSequence());
    final S3ObjectId infoObj = AwsUtils.key(archiveObj, table, downloadFile.getName());

    // Move the object to the archive bucket.
    aws.getClient().putObject(infoObj.getBucket(), infoObj.getKey(), downloadFile);
    log.info("Uploaded " + downloadFile + " to " + infoObj);
    downloadFile.delete();
  }

  public S3ObjectId finalizeDump(final DataDump dump, final CanvasDataSchema schema)
      throws IOException {
    final S3ObjectId archiveObj = getArchiveDumpObj(dump.getSequence());
    aws.writeJson(AwsUtils.key(archiveObj, "schema.json"), schema);
    aws.writeJson(AwsUtils.key(archiveObj, "dump_info.json"), dump);
    final Set<String> dirs = new HashSet<String>();
    for (final S3ObjectId directory : aws.listDirectories(archiveObj)) {
      dirs.add(directory.getKey());
    }
    // XXX We can probably get rid of the empty_files; if we generate the S3
    // copy scripts to take advantage of file lists we can be more precise
    // about what directories we try to copy.
    for (final DataSchemaTable table : schema.getTables().values()) {
      final S3ObjectId tableKey = AwsUtils.key(archiveObj, table.getTableName());
      if (!dirs.contains(tableKey.getKey())) {
        System.out.println("Table " + tableKey + " missing");
        aws.writeEmptyFile(AwsUtils.key(tableKey, "empty_file"));
      }
    }
    return archiveObj;
  }

  public void deleteTemporaryDump(final DataDump dump) throws IOException {
    final File directory = getScratchDumpDir(dump);
    FileUtils.deleteDirectory(directory);
  }

  private File getScratchDumpDir(final DataDump dump) {
    final String dirName = String.format("%05d", dump.getSequence());
    return new File(config.getScratchDir(), dirName);
  }

  public S3ObjectId getArchiveDumpObj(final long dumpSequence) {
    final String dirName = String.format("%05d", dumpSequence);
    return AwsUtils.key(config.getS3IncomingLocation(), dirName);
  }

  public void updateTableInfoTable(final DataDump dump) {
    for (final DataArtifact artifact : dump.getArtifactsByTable().values()) {
      final String tableName = artifact.getTableName();
      final boolean partial = artifact.isPartial();
      if (!partial) {
        final TableInfo info = new TableInfo(tableName, dump.getDumpId(), dump.getSequence());
        info.save();
      }
    }
  }

  public Map<String, List<S3ObjectId>> getDumpIndex(final long dumpSequence) {
    final Map<String, List<S3ObjectId>> directories = new HashMap<String, List<S3ObjectId>>();
    final S3ObjectId dumpDir = getArchiveDumpObj(dumpSequence);
    for (final S3ObjectId tableDir : aws.listDirectories(dumpDir)) {
      if (!aws.isFile(AwsUtils.key(tableDir, "empty_file"))) {
        final String tableName = tableDir.getKey()
            .substring(tableDir.getKey().lastIndexOf("/") + 1);
        final List<S3ObjectId> tableDirs = new ArrayList<S3ObjectId>();
        tableDirs.add(tableDir);
        directories.put(tableName, tableDirs);
      }
    }
    return directories;
  }
}
