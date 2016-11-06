package edu.harvard.data.canvas.phase_0;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataTable;
import edu.harvard.data.DumpInfo;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.bindings.phase_0.Phase0CanvasTable;

public class Phase0PostVerifier {

  private static final Logger log = LogManager.getLogger();
  private final String dumpId;
  private final AwsUtils aws;
  private final ExecutorService exec;
  private final File tmpDir;
  private final TableFormat format;

  public Phase0PostVerifier(final String dumpId, final AwsUtils aws, final CanvasDataConfig config,
      final ExecutorService exec) {
    this.dumpId = dumpId;
    this.aws = aws;
    this.tmpDir = new File(config.getScratchDir());
    this.exec = exec;
    this.format = new FormatLibrary().getFormat(Format.CanvasDataFlatFiles);
  }

  public void verify() throws VerificationException, IOException {
    final DumpInfo info = DumpInfo.find(dumpId);
    if (!info.getDownloaded()) {
      throw new VerificationException("Dump " + dumpId + " has not been downloaded");
    }
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

  private long verifyDump(final S3ObjectId dumpObj) throws IOException, VerificationException {
    final Set<Future<Long>> futures = new HashSet<Future<Long>>();
    for (final S3ObjectId dir : aws.listDirectories(dumpObj)) {
      final String tableName = dir.getKey().substring(dir.getKey().lastIndexOf("/") + 1);
      // XXX Previous versions of the archiving code stored the identity map
      // along with the data from Canvas. Once the archives have been cleaned
      // out, and that code is no longer used, we can get rid of this
      // conditional.
      if (!tableName.equals("identity_map")) {
        final Phase0CanvasTable table = Phase0CanvasTable.fromSourceName(tableName);
        for (final S3ObjectSummary file : aws.listKeys(dir)) {
          final S3ObjectId awsFile = AwsUtils.key(file.getBucketName(), file.getKey());
          log.info("Verifying S3 file " + file.getBucketName() + "/" + file.getKey()
          + " representing table " + table);
          final Callable<Long> job = new CanvasPhase0VerifierJob2(aws, awsFile, format, table,
              tmpDir);
          final Future<Long> future = exec.submit(job);
          futures.add(future);
        }
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
        } else if (e.getCause() instanceof VerificationException) {
          throw (VerificationException) e.getCause();
        } else {
          log.error("Caught unexpected error from verify job", e.getCause());
          throw new VerificationException(e.getCause());
        }
      }
    }
    return errorCount;
  }
}

class CanvasPhase0VerifierJob2 implements Callable<Long> {
  private static final int MAX_LOG_LINES = 100;
  private static final Logger log = LogManager.getLogger();
  private final S3ObjectId awsFile;
  private final Phase0CanvasTable table;
  private final File tmpDir;
  private final AwsUtils aws;
  private final TableFormat format;

  public CanvasPhase0VerifierJob2(final AwsUtils aws, final S3ObjectId awsFile,
      final TableFormat format, final Phase0CanvasTable table, final File tmpDir) {
    this.aws = aws;
    this.awsFile = awsFile;
    this.format = format;
    this.table = table;
    this.tmpDir = tmpDir;
  }

  @Override
  public Long call() throws IOException, VerificationException {
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
        final List<String> errors = verifyFile(table.getTableClass(), tmpFile);
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

  private <T extends DataTable> List<String> verifyFile(final Class<T> cls, final File tmpFile)
      throws IOException, VerificationException {
    Constructor<T> constructor;
    try {
      constructor = cls.getConstructor(TableFormat.class, CSVRecord.class);
    } catch (NoSuchMethodException | SecurityException e) {
      log.error("Failed to load constructor for class " + cls.getCanonicalName(), e);
      throw new VerificationException(e);
    }
    final List<String> differences = new ArrayList<String>();
    int linesPrinted = 0;
    try (final BufferedReader in = getReaderForFile(tmpFile);) {
      String line = in.readLine();
      while (line != null) {
        line += "\n"; // put back the newline that in.readLine stripped.
        final StringBuilder parsedLine = new StringBuilder();
        final CSVPrinter printer = format.getCsvFormat().print(parsedLine);
        for (final CSVRecord csvRecord : CSVParser.parse(line, format.getCsvFormat())
            .getRecords()) {
          T record;
          try {
            record = constructor.newInstance(format, csvRecord);
          } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
              | InvocationTargetException e) {
            log.error("Failed to create instance of " + cls.getCanonicalName(), e);
            log.info("File: " + tmpFile);
            log.info("Line: " + line);
            log.info("Record: " + csvRecord);
            throw new VerificationException(e);
          }
          printer.printRecord(record.getFieldsAsList(format));
        }
        final String cleanOriginal = cleanLine(line);
        final String cleanParsed = cleanLine(parsedLine.toString());
        if (!cleanOriginal.equals(cleanParsed)) {
          differences.add("< " + cleanOriginal + "\n> " + cleanParsed);
          if (linesPrinted++ < MAX_LOG_LINES) {
            log.debug("Difference found in " + tmpFile.getName() + ":\n"
                + differences.get(differences.size() - 1));
          }
        }
        line = in.readLine();
      }
    }

    return differences;
  }

  private String cleanLine(String line) throws IOException {
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