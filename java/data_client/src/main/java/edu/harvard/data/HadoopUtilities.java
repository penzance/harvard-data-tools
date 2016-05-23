package edu.harvard.data;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.io.CombinedTableReader;
import edu.harvard.data.io.HdfsTableReader;
import edu.harvard.data.io.TableReader;

public class HadoopUtilities {
  private static final Logger log = LogManager.getLogger();

  public void setPaths(final Job job, final URI hdfsService, final String in, final String out)
      throws IOException {
    for (final Path path : listFiles(hdfsService, in)) {
      FileInputFormat.addInputPath(job, path);
      log.debug("Input path: " + path.toString());
    }

    if (out == null) {
      log.info("Job has not specified an output path. Sending to dummy location.");
      FileOutputFormat.setOutputPath(job, new Path("/dev/null"));
    } else {
      log.debug("Output path: " + out);
      FileOutputFormat.setOutputPath(job, new Path(out));
    }
  }

  public List<Path> listFiles(final URI hdfsService, final String dir) throws IOException {
    final List<Path> paths = new ArrayList<Path>();
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(hdfsService, conf);
    for (final FileStatus status : fs.listStatus(new Path(dir))) {
      if (!getFileName(status.getPath()).startsWith("_")) {
        paths.add(status.getPath());
      }
    }
    return paths;
  }

  public String getFileName(final Path path) {
    final String filename = path.toString();
    return filename.substring(filename.lastIndexOf("/") + 1, filename.length());
  }

  public void addToCache(final Job job, final URI hdfsService, final String dir)
      throws IOException {
    for (final Path path : listFiles(hdfsService, dir)) {
      job.addCacheFile(URI.create(path.toString()));
      log.debug("Adding cache file: " + path.toString());
    }
  }

  public Text convertToText(final DataTable record, final TableFormat format) throws IOException {
    final StringWriter writer = new StringWriter();
    try (final CSVPrinter printer = new CSVPrinter(writer, format.getCsvFormat())) {
      printer.printRecord(record.getFieldsAsList(format));
    }
    return new Text(writer.toString().replaceAll("\n", ""));
  }

  public Text recordToText(final DataTable record, final TableFormat format)
      throws IOException, InterruptedException {
    final StringWriter writer = new StringWriter();
    try (final CSVPrinter printer = new CSVPrinter(writer, format.getCsvFormat())) {
      printer.printRecord(record.getFieldsAsList(format));
    }
    return new Text(writer.toString().replaceAll("\n", ""));
  }

  public TableFormat getFormat(final Reducer<?, ?, ?, ?>.Context context) {
    return parseFormat(context.getConfiguration().get("format"));
  }

  public TableFormat getFormat(final Mapper<?, ?, ?, ?>.Context context) {
    return parseFormat(context.getConfiguration().get("format"));
  }

  private TableFormat parseFormat(final String formatString) {
    if (formatString == null) {
      throw new HadoopConfigurationException(
          "Required Hadoop configuration parameter 'format' missing");
    }
    final Format formatName;
    try {
      formatName = Format.valueOf(formatString);
    } catch (final IllegalArgumentException e) {
      throw new HadoopConfigurationException(
          "Unknown file format: \"" + formatString + "\". Expected one of " + Format.values(), e);
    }
    final TableFormat format = new FormatLibrary().getFormat(formatName);
    if (format == null) {
      throw new HadoopConfigurationException("Unknown format " + formatName);
    }
    return format;
  }

  public IdentifierType getMainIdentifier(final Reducer<?, ?, ?, ?>.Context context) {
    return parseMainIdentifier(context.getConfiguration().get("mainIdentifier"));
  }

  public IdentifierType getMainIdentifier(final Mapper<?, ?, ?, ?>.Context context) {
    return parseMainIdentifier(context.getConfiguration().get("mainIdentifier"));
  }

  private IdentifierType parseMainIdentifier(final String idString) {
    if (idString == null) {
      throw new HadoopConfigurationException(
          "Required Hadoop configuration parameter 'mainIdentifier' missing");
    }
    try {
      return IdentifierType.valueOf(idString);
    } catch (final IllegalArgumentException e) {
      throw new HadoopConfigurationException("Unknown main identifier type: \"" + idString
          + "\". Expected one of " + IdentifierType.values(), e);
    }
  }

  public <T extends DataTable> TableReader<T> getHdfsTableReader(
      final Mapper<?, ?, ?, ?>.Context context, final TableFormat format, final Class<T> tableType)
          throws IOException {
    return getHdfsTableReader(context.getConfiguration(), context.getCacheFiles(), format,
        tableType);
  }

  public <T extends DataTable> TableReader<T> getHdfsTableReader(
      final Reducer<?, ?, ?, ?>.Context context, final TableFormat format, final Class<T> tableType)
          throws IOException {
    return getHdfsTableReader(context.getConfiguration(), context.getCacheFiles(), format,
        tableType);
  }

  public <T extends DataTable> TableReader<T> getHdfsTableReader(final Configuration config,
      final URI[] cacheFiles, final TableFormat format, final Class<T> tableType)
          throws IOException {
    final FileSystem fs = FileSystem.get(config);
    final List<TableReader<T>> readers = new ArrayList<TableReader<T>>();
    for (final URI uri : cacheFiles) {
      final Path path = new Path(uri.toString());
      readers.add(new HdfsTableReader<T>(tableType, format, fs, path));
    }
    return new CombinedTableReader<T>(readers);
  }

}
