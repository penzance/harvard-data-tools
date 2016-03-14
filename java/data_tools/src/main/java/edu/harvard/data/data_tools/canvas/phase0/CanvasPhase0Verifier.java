package edu.harvard.data.data_tools.canvas.phase0;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.harvard.data.client.AwsUtils;
import edu.harvard.data.client.DataTable;
import edu.harvard.data.client.TableFactory;
import edu.harvard.data.client.TableFormat;
import edu.harvard.data.client.canvas.phase0.CanvasTable;
import edu.harvard.data.client.io.TableReader;
import edu.harvard.data.client.io.TableWriter;
import edu.harvard.data.data_tools.DumpInfo;
import edu.harvard.data.data_tools.VerificationException;
import edu.harvard.data.data_tools.Verifier;

public class CanvasPhase0Verifier implements Verifier {

  private static final Logger log = LogManager.getLogger();
  private final String dumpId;
  private final AwsUtils aws;
  private final ExecutorService exec;
  private final File tmpDir;
  private final TableFactory factory;
  private final TableFormat format;

  public CanvasPhase0Verifier(final String dumpId, final AwsUtils aws, final TableFactory factory,
      final TableFormat format, final File tmpDir, final ExecutorService exec) {
    this.dumpId = dumpId;
    this.aws = aws;
    this.factory = factory;
    this.format = format;
    this.tmpDir = tmpDir;
    this.exec = exec;
  }

  @Override
  public void verify() throws VerificationException, IOException {
    final DumpInfo info = DumpInfo.find(dumpId);
    if (!info.getVerified()) {
      log.info("Verifying dump sequence " + info.getSequence() + " at " + info.getS3Location());
      final S3ObjectId dumpObj = AwsUtils.key(info.getBucket(), info.getKey());
      final long errors = verifyDump(dumpObj);
      if (errors > 0) {
        throw new VerificationException(
            "Encountered " + errors + " errors when verifying dump at " + dumpObj);
      }
      info.setVerified(true);
      info.save();
    }
  }

  private long verifyDump(final S3ObjectId dumpObj) throws IOException {
    final Set<Future<Long>> futures = new HashSet<Future<Long>>();
    for (final S3ObjectId dir : aws.listDirectories(dumpObj)) {
      final String tableName = dir.getKey().substring(dir.getKey().lastIndexOf("/") + 1);
      final CanvasTable table = CanvasTable.fromSourceName(tableName);
      for (final S3ObjectSummary file : aws.listKeys(dir)) {
        final S3ObjectId awsFile = AwsUtils.key(file.getBucketName(), file.getKey());
        log.info("Verifying S3 file " + file.getBucketName() + "/" + file.getKey()
        + " representing table " + table);
        final Callable<Long> job = new CanvasPhase0VerifierJob(aws, awsFile, factory, format, table,
            tmpDir);
        final Future<Long> future = exec.submit(job);
        futures.add(future);
      }
    }
    long errorCount = 0;
    for (final Future<Long> future : futures) {
      try {
        errorCount += future.get();
      } catch (final InterruptedException e) {
        log.error("Interrupted while waiting for verify tasks to complete", e);
        return errorCount + 1; // Ensure that we don't mark the verify as
        // successful.
      } catch (final ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        } else {
          log.error("Caught unexpected error from verify job", e);
          throw new RuntimeException(e);
        }
      }
    }
    return errorCount;
  }
}

class CanvasPhase0VerifierJob implements Callable<Long> {
  private static final int MAX_LOG_LINES = 100;
  private static final Logger log = LogManager.getLogger();
  private final S3ObjectId awsFile;
  private final CanvasTable table;
  private final File tmpDir;
  private final AwsUtils aws;
  private final TableFactory factory;
  private final TableFormat format;

  public CanvasPhase0VerifierJob(final AwsUtils aws, final S3ObjectId awsFile,
      final TableFactory factory, final TableFormat format, final CanvasTable table,
      final File tmpDir) {
    this.aws = aws;
    this.awsFile = awsFile;
    this.factory = factory;
    this.format = format;
    this.table = table;
    this.tmpDir = tmpDir;
  }

  @Override
  public Long call() throws IOException {
    log.info("Running verifier job for " + awsFile);
    final String path;
    String fileName;
    if (awsFile.getKey().contains("/")) {
      fileName = awsFile.getKey().substring(awsFile.getKey().lastIndexOf("/"));
      path = awsFile.getKey().substring(0, awsFile.getKey().lastIndexOf("/"));
    } else {
      fileName = awsFile.getKey();
      path = "/";
    }
    final File tmpFile = new File(tmpDir, path + "/" + fileName);
    log.info("Temp file: " + tmpFile);

    try {
      aws.getFile(awsFile, tmpFile);
      long errorCount = 0;
      if (tmpFile.length() > 0) {
        final List<String> errors = verifyFile(tmpFile, table);
        if (!errors.isEmpty()) {
          errorCount += errors.size();
        }
      }
      log.info("Found " + errorCount + " errors in file " + awsFile);

      return errorCount;
    } finally {
      tmpFile.delete();
    }

  }

  @SuppressWarnings("unchecked")
  private <T extends DataTable> List<String> verifyFile(final File tmpFile,
      final CanvasTable table2) throws IOException {
    final File parsedFile = new File(
        tmpFile.getParent() + File.separator + "parsed-" + tmpFile.getName());
    try (
        final TableReader<T> in = (TableReader<T>) factory.getTableReader(table.getSourceName(),
            format, tmpFile);
        final TableWriter<T> out = (TableWriter<T>) factory.getTableWriter(table.getSourceName(),
            format, parsedFile)) {
      out.pipe(in);
    }
    final List<String> differences = textualCompareFiles(tmpFile, parsedFile);
    parsedFile.delete();
    return differences;
  }

  private List<String> textualCompareFiles(final File file, final File parsedFile)
      throws IOException {
    final Map<String, Integer> lineMap = buildLineMap(file);
    final List<String> errors = removeMatchingLines(lineMap, parsedFile);
    int linesPrinted = 0;
    for (final String line : lineMap.keySet()) {
      for (int i = 0; i < lineMap.get(line); i++) {
        errors.add("< " + line);
        if (linesPrinted++ < MAX_LOG_LINES) {
          log.debug(errors.get(errors.size() - 1));
        }
      }
    }
    return errors;
  }

  private Map<String, Integer> buildLineMap(final File file) throws IOException {
    final Map<String, Integer> lineMap = new HashMap<String, Integer>();
    try (BufferedReader in = getReaderForFile(file)) {
      String line = readAndCleanLine(in);
      while (line != null) {
        if (lineMap.containsKey(line)) {
          lineMap.put(line, lineMap.get(line) + 1);
        } else {
          lineMap.put(line, 1);
        }
        line = readAndCleanLine(in);
      }
    }
    return lineMap;
  }

  private List<String> removeMatchingLines(final Map<String, Integer> lineMap,
      final File parsedFile) throws IOException {
    final List<String> errors = new ArrayList<String>();
    int linesPrinted = 0;
    try (BufferedReader in = getReaderForFile(parsedFile)) {
      String line = readAndCleanLine(in);
      while (line != null) {
        if (!lineMap.containsKey(line)) {
          errors.add("> " + line);
          if (linesPrinted++ < MAX_LOG_LINES) {
            log.debug(errors.get(errors.size() - 1));
          }
        } else {
          if (lineMap.get(line) == 1) {
            lineMap.remove(line);
          } else {
            lineMap.put(line, lineMap.get(line) - 1);
          }
        }
        line = readAndCleanLine(in);
      }
    }
    return errors;
  }

  private String readAndCleanLine(final BufferedReader in) throws IOException {
    String line = in.readLine();
    if (line != null) {
      line = line.replaceAll("\\\\N", "\\\\n");
      line = line.replaceAll("\\.0\\t", "\t");
    }
    return line;
  }

  private static BufferedReader getReaderForFile(final File file)
      throws FileNotFoundException, IOException {
    InputStream in;
    if (file.getName().toLowerCase().endsWith(".gz")) {
      in = new GZIPInputStream(new FileInputStream(file));
    } else {
      in = new FileInputStream(file);
    }
    return new BufferedReader(new InputStreamReader(in));
  }

}