package edu.harvard.data;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

  protected void setPaths(final Job job, final AwsUtils aws, final URI hdfsService,
      final String in, final String out) throws IOException {
    for(final Path path : listFiles(in)){
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

  List<Path> listFiles(final String dir) throws IOException {
    final List<Path> paths = new ArrayList<Path>();
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(hdfsService, conf);
    for (final FileStatus status : fs.listStatus(new Path(dir))) {
      String filename = status.getPath().toString();
      filename = filename.substring(filename.lastIndexOf("/") + 1, filename.length());
      if (!filename.startsWith("_")) {
        paths.add(status.getPath());
      }
    }
    return paths;
  }

  protected void addToCache(final Job job, final String dir) throws IOException {
    for(final Path path : listFiles(dir)){
      job.addCacheFile(URI.create(path.toString()));
      log.debug("Adding cache file: " + path.toString());
    }
  }

  public abstract Job getJob() throws IOException;
}
