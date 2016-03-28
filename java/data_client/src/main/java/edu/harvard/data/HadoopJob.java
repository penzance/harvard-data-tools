package edu.harvard.data;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public abstract class HadoopJob {

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
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(hdfsService, conf);
    final FileStatus[] fileStatus = fs.listStatus(new Path(in));
    for(final FileStatus status : fileStatus){
      FileInputFormat.addInputPath(job, status.getPath());
      System.out.println("Input path: " + status.getPath().toString());
    }

    System.out.println("Output path: " + out);
    FileOutputFormat.setOutputPath(job, new Path(out));
  }

  public abstract Job getJob() throws IOException;
}
