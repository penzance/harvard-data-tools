package edu.harvard.data.canvas.phase_0;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
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
  public void saveDump(final ApiClient api, final DataDump dump, final ExecutorService exec)
      throws IOException, UnexpectedApiResponseException, VerificationException, ArgumentError {
    final File directory = getScratchDumpDir(dump);
    final boolean created = directory.mkdirs();
    if (!created) {
      throw new IOException("Failed to create directory " + directory);
    }
    final List<Future<Void>> futures = new ArrayList<Future<Void>>();
    final Map<String, DataArtifact> artifactsByTable = dump.getArtifactsByTable();
    for (final String table : artifactsByTable.keySet()) {
      int fileIndex = 0;
      for (int i = 0; i < artifactsByTable.get(table).getFiles().size(); i++) {
        final File tempDir = new File(directory, table);
        final DataFile file = artifactsByTable.get(table).getFiles().get(fileIndex);
        final DownloadTask task = new DownloadTask(config, api, dump.getDumpId(), table,
            file.getFilename(), tempDir, fileIndex);
        fileIndex++;
        futures.add(exec.submit(task));
      }
    }
    int downloadedFiles = 0;
    for (final Future<Void> future : futures) {
      try {
        future.get();
      } catch (final InterruptedException e) {
        log.fatal("Interrupted while waiting for future", e);
        throw new RuntimeException(e);
      } catch (final ExecutionException e) {
        final Throwable t = e.getCause();
        if (t instanceof IOException) {
          throw (IOException) t;
        }
        if (t instanceof UnexpectedApiResponseException) {
          throw (UnexpectedApiResponseException) t;
        }
        if (t instanceof ArgumentError) {
          throw (ArgumentError) t;
        }
        log.fatal("Unexpected error.", e);
        throw new RuntimeException(t);
      }
      downloadedFiles++;
    }
    if (downloadedFiles != dump.countFilesToDownload()) {
      throw new VerificationException("Expected to download " + dump.countFilesToDownload()
      + " files. Actually downloaded " + downloadedFiles);
    }
  }

  public S3ObjectId finalizeDump(final DataDump dump, final CanvasDataSchema schema)
      throws IOException {
    final S3ObjectId archiveObj = getArchiveDumpObj(config.getArchiveLocation(),
        dump.getSequence());
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

  public S3ObjectId getArchiveDumpObj(final S3ObjectId baseDir, final long dumpSequence) {
    final String dirName = String.format("%05d", dumpSequence);
    return AwsUtils.key(baseDir, dirName);
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

  public Map<String, List<S3ObjectId>> getDumpIndex(final S3ObjectId dumpDir) {
    final Map<String, List<S3ObjectId>> directories = new HashMap<String, List<S3ObjectId>>();
    log.info("Getting dump index for " + dumpDir);
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
