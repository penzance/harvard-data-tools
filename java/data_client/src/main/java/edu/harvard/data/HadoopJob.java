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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.identity.HadoopIdentityKey;

public abstract class HadoopJob {
  private static final Logger log = LogManager.getLogger();

  protected Configuration hadoopConf;
  protected AwsUtils aws;
  protected String inputDir;
  protected String outputDir;
  protected URI hdfsService;

  public HadoopJob(final Configuration hadoopConf, final AwsUtils aws, final URI hdfsService,
      final String inputDir, final String outputDir) {
    this.hadoopConf = hadoopConf;
    this.aws = aws;
    this.hdfsService = hdfsService;
    this.inputDir = inputDir;
    this.outputDir = outputDir;
  }

  public abstract Job getJob() throws IOException;

  public static void setPaths(final Job job, final AwsUtils aws, final URI hdfsService,
      final String in, final String out) throws IOException {
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

  public static List<Path> listFiles(final URI hdfsService, final String dir) throws IOException {
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

  public static String getFileName(final Path path) {
    final String filename = path.toString();
    return filename.substring(filename.lastIndexOf("/") + 1, filename.length());
  }

  protected void addToCache(final Job job, final String dir) throws IOException {
    for (final Path path : listFiles(hdfsService, dir)) {
      job.addCacheFile(URI.create(path.toString()));
      log.debug("Adding cache file: " + path.toString());
    }
  }

  public static Text convertToText(final DataTable record, final TableFormat format)
      throws IOException {
    final StringWriter writer = new StringWriter();
    try (final CSVPrinter printer = new CSVPrinter(writer, format.getCsvFormat())) {
      printer.printRecord(record.getFieldsAsList(format));
    }
    return new Text(writer.toString().trim());
  }

  public static Text recordToText(final DataTable record, final TableFormat format)
      throws IOException, InterruptedException {
    final StringWriter writer = new StringWriter();
    try (final CSVPrinter printer = new CSVPrinter(writer, format.getCsvFormat())) {
      printer.printRecord(record.getFieldsAsList(format));
    }
    return new Text(writer.toString().trim());
  }

  public static TableFormat getFormat(
      final Reducer<?, HadoopIdentityKey, Text, NullWritable>.Context context) {
    final String formatString = context.getConfiguration().get("format");
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
}
