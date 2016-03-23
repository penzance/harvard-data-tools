package edu.harvard.data.canvas.phase_1;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.harvard.data.canvas.identity.CanvasIdentityHadoopManager;
import edu.harvard.data.identity.HadoopIdentityKey;
import edu.harvard.data.identity.IdentityReducer;

public class Phase1HadoopManager {

  private static final Path TEMP_HDFS_DIR = new Path("/identity/unknown");

  private final String inputDir;
  private final String outputDir;
  private final URI hdfsService;
  private final CanvasIdentityHadoopManager hadoopManager;

  public Phase1HadoopManager(final String inputDir, final String outputDir, final URI hdfsService) {
    this.inputDir = inputDir;
    this.outputDir = outputDir;
    this.hdfsService = hdfsService;
    hadoopManager = new CanvasIdentityHadoopManager();
  }

  @SuppressWarnings("rawtypes")
  public void runMapJobs(final Configuration hadoopConfig) throws IOException {
    final Job job = Job.getInstance(hadoopConfig, "canvas-identity-map");
    job.setJarByClass(Phase1HadoopManager.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setReducerClass(IdentityReducer.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(HadoopIdentityKey.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    for (final Path path : listHdfsFiles(hadoopConfig, new Path(inputDir + "/id"))) {
      job.addCacheFile(path.toUri());
    }

    final List<String> tables = hadoopManager.getIdentityTableNames();
    final List<Class<? extends Mapper>> mapperClasses = hadoopManager.getMapperClasses();
    for (int i = 0; i < mapperClasses.size(); i++) {
      final Path path = new Path(inputDir + "/" + tables.get(i) + "/");
      MultipleInputs.addInputPath(job, path, TextInputFormat.class, mapperClasses.get(i));
    }
    FileOutputFormat.setOutputPath(job, TEMP_HDFS_DIR);
    try {
      job.waitForCompletion(true);
    } catch (ClassNotFoundException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  public void runScrubJobs(final Configuration hadoopConfig) throws IOException {
    final List<String> tables = hadoopManager.getIdentityTableNames();
    final List<Class<? extends Mapper>> scrubberClasses = hadoopManager.getScrubberClasses();
    final List<Job> jobs = new ArrayList<Job>();
    for (int i = 0; i < scrubberClasses.size(); i++) {
      jobs.add(buildScrubJob(hadoopConfig, tables.get(i), scrubberClasses.get(i)));
    }

    try {
      for (final Job job : jobs) {
        job.waitForCompletion(true);
      }
    } catch (ClassNotFoundException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  private Job buildScrubJob(final Configuration hadoopConfig, final String tableName,
      final Class<? extends Mapper> cls) throws IOException {
    final Job job = Job.getInstance(hadoopConfig, "canvas-" + tableName + "-scrubber");
    job.setJarByClass(Phase1HadoopManager.class);
    job.setMapperClass(cls);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job, hdfsService, inputDir + "/" + tableName, outputDir + "/" + tableName);
    for (final Path path : listHdfsFiles(hadoopConfig, TEMP_HDFS_DIR)) {
      job.addCacheFile(path.toUri());
    }
    return job;
  }

  private List<Path> listHdfsFiles(final Configuration hadoopConfig, final Path path)
      throws IOException {
    final List<Path> files = new ArrayList<Path>();
    final FileSystem fs = FileSystem.get(hadoopConfig);
    for (final FileStatus fileStatus : fs.listStatus(path)) {
      files.add(fileStatus.getPath());
    }
    return files;
  }

  private void setPaths(final Job job, final URI hdfsService, final String in, final String out)
      throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(hdfsService, conf);
    final FileStatus[] fileStatus = fs.listStatus(new Path(in));
    for (final FileStatus status : fileStatus) {
      FileInputFormat.addInputPath(job, status.getPath());
      System.out.println("Input path: " + status.getPath().toString());
    }

    System.out.println("Output path: " + out);
    FileOutputFormat.setOutputPath(job, new Path(out));
  }

}
