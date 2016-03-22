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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.harvard.data.canvas.identity.PseudonymDimIdentityMapper;
import edu.harvard.data.canvas.identity.PseudonymDimIdentityScrubber;
import edu.harvard.data.canvas.identity.RequestsIdentityMapper;
import edu.harvard.data.canvas.identity.RequestsIdentityScrubber;
import edu.harvard.data.identity.HadoopIdentityKey;
import edu.harvard.data.identity.IdentityReducer;

public class Phase1HadoopManager {

  private final String inputDir;
  private final String outputDir;
  private final URI hdfsService;

  public Phase1HadoopManager(final String inputDir, final String outputDir, final URI hdfsService) {
    this.inputDir = inputDir;
    this.outputDir = outputDir;
    this.hdfsService = hdfsService;
  }

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

    for (final Path path : listHdfsFiles(hadoopConfig, "/phase_0/id")) {
      job.addCacheFile(path.toUri());
    }

    MultipleInputs.addInputPath(job, new Path("/phase_0/requests/"), TextInputFormat.class,
        RequestsIdentityMapper.class);
    MultipleInputs.addInputPath(job, new Path("/phase_0/pseudonym_dim/"), TextInputFormat.class,
        PseudonymDimIdentityMapper.class);

    FileOutputFormat.setOutputPath(job, new Path("/identity/unknown"));

    try {
      job.waitForCompletion(true);
    } catch (ClassNotFoundException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    listHdfsFiles(hadoopConfig, "/identity/unknown");
  }

  private List<Path> listHdfsFiles(final Configuration hadoopConfig, final String path)
      throws IOException {
    final List<Path> files = new ArrayList<Path>();
    final FileSystem fs = FileSystem.get(hadoopConfig);
    for (final FileStatus fileStatus : fs.listStatus(new Path(path))) {
      files.add(fileStatus.getPath());
    }
    return files;
  }

  public void runScrubJobs(final Configuration hadoopConfig) throws IOException {
    final Job job1 = Job.getInstance(hadoopConfig, "canvas-requests-scrubber");
    job1.setJarByClass(Phase1HadoopManager.class);
    job1.setMapperClass(RequestsIdentityScrubber.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(NullWritable.class);
    job1.setNumReduceTasks(0);

    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job1, hdfsService, inputDir + "/requests", outputDir + "/requests");
    for (final Path path : listHdfsFiles(hadoopConfig, "/identity/unknown")) {
      job1.addCacheFile(path.toUri());
    }

    final Job job2 = Job.getInstance(hadoopConfig, "canvas-pseudonym-dim-scrubber");
    job2.setJarByClass(Phase1HadoopManager.class);
    job2.setMapperClass(PseudonymDimIdentityScrubber.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(NullWritable.class);
    job2.setNumReduceTasks(0);

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    setPaths(job2, hdfsService, inputDir + "/pseudonym_dim", outputDir + "/pseudonym_dim");
    for (final Path path : listHdfsFiles(hadoopConfig, "/identity/unknown")) {
      job2.addCacheFile(path.toUri());
    }

    try {
      job1.waitForCompletion(true);
      job2.waitForCompletion(true);
    } catch (ClassNotFoundException | InterruptedException e) {
      throw new RuntimeException(e);
    }
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
