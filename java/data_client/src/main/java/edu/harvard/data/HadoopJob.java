package edu.harvard.data;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public abstract class HadoopJob {

  protected Configuration hadoopConf;
  protected AwsUtils aws;
  protected String inputDir;
  protected String outputDir;
  protected HadoopUtilities hadoopUtils;
  protected final URI hdfsService;

  public HadoopJob(final Configuration hadoopConf, final AwsUtils aws, final URI hdfsService,
      final String inputDir, final String outputDir) {
    this.hadoopConf = hadoopConf;
    this.aws = aws;
    this.inputDir = inputDir;
    this.outputDir = outputDir;
    this.hdfsService = hdfsService;
    this.hadoopUtils = new HadoopUtilities();
  }

  public abstract Job getJob() throws IOException;

}
