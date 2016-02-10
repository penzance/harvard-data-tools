package edu.harvard.canvas_data.aws_data_tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class Verifier {

  private static final Logger log = LogManager.getLogger();

  private static final int MAX_LOG_LINES = 100;

  private final TableFormat format;
  private final TableFactory factory;
  private final AwsUtils aws;

  public Verifier(final AwsUtils aws, final TableFactory factory, final TableFormat format) {
    this.aws = aws;
    this.factory = factory;
    this.format = format;
  }

  public long verifyDump(final S3ObjectId dumpObj) throws IOException {
    long errorCount = 0;
    for (final S3ObjectId dir : aws.listDirectories(dumpObj)) {
      final String tableName = dir.getKey().substring(dir.getKey().lastIndexOf("/") + 1);
      final CanvasTable table = CanvasTable.fromSourceName(tableName);
      for (final S3ObjectSummary file : aws.listKeys(dir)) {
        log.info("Verifying S3 file " + file.getBucketName() + "/" + file.getKey());
        final File tempFile = new File(
            "/tmp/tempfile" + file.getKey().substring(file.getKey().lastIndexOf(".")));
        aws.getFile(AwsUtils.key(file.getBucketName(), file.getKey()), tempFile);
        final List<String> errors = verifyFile(tempFile, tableName, table.getTableClass());
        tempFile.delete();
        if (!errors.isEmpty()) {
          log.info("  Found " + errors.size() + " errors");
          errorCount += errors.size();
        }
      }
    }
    return errorCount;
  }

  @SuppressWarnings("unchecked")
  private <T extends DataTable> List<String> verifyFile(final File tempFile, final String table,
      final Class<? extends DataTable> class1) throws IOException {
    final File parsedFile = new File(
        tempFile.getParent() + File.separator + "parsed-" + tempFile.getName());
    try (final TableReader<T> in = (TableReader<T>) factory.getTableReader(table, format, tempFile);
        final TableWriter<T> out = (TableWriter<T>) factory.getTableWriter(table, format,
            parsedFile)) {
      out.pipe(in);
    }
    final List<String> differences = textualCompareFiles(tempFile, parsedFile);
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
