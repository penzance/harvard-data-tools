package edu.harvard.data.identity;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfig;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopUtilities;
import edu.harvard.data.generator.GeneratedCodeManager;

public class IdentityMapHadoopJob {
  private static final Logger log = LogManager.getLogger();

  private final DataConfig config;
  private final String inputDir;
  private final String outputDir;
  private final Configuration hadoopConfig;
  final HadoopUtilities hadoopUtils;

  private final GeneratedCodeManager codeManager;

  public IdentityMapHadoopJob(final DataConfig config, final GeneratedCodeManager codeManager)
      throws IOException {
    this.config = config;
    this.codeManager = codeManager;
    this.inputDir = config.getHdfsDir(0);
    this.outputDir = config.getHdfsDir(1);
    this.hadoopConfig = new Configuration();
    this.hadoopUtils = new HadoopUtilities();
  }

  @SuppressWarnings("rawtypes")
  protected void run() throws IOException {
    final IdentifierType mainIdentifier = config.mainIdentifier;
    hadoopConfig.set("format", Format.DecompressedCanvasDataFlatFiles.toString());
    hadoopConfig.set("mainIdentifier", config.mainIdentifier.toString());
    final Job job = Job.getInstance(hadoopConfig, "identity-map");
    job.setJarByClass(IdentityMapHadoopJob.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    if (mainIdentifier.getType() == Long.class) {
      job.setReducerClass(LongIdentityReducer.class);
      job.setMapOutputKeyClass(LongWritable.class);
    } else if (mainIdentifier.getType() == String.class) {
      job.setReducerClass(StringIdentityReducer.class);
      job.setMapOutputKeyClass(Text.class);
    } else {
      throw new RuntimeException("Unknown main identifier type: " + mainIdentifier.getType());
    }
    job.setMapOutputValueClass(HadoopIdentityKey.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    for (final Path path : hadoopUtils.listHdfsFiles(hadoopConfig,
        new Path(inputDir + "/identity_map"))) {
      log.info("Adding identity file " + path + " to map job cache");
      job.addCacheFile(path.toUri());
    }

    final List<String> tables = codeManager.getIdentityTableNames();
    final List<Class<? extends Mapper>> mapperClasses = codeManager.getIdentityMapperClasses();
    for (int i = 0; i < mapperClasses.size(); i++) {
      final Path path = new Path(inputDir + "/" + tables.get(i) + "/");
      MultipleInputs.addInputPath(job, path, TextInputFormat.class, mapperClasses.get(i));
      log.info("Adding mapper for path " + path);
    }
    FileOutputFormat.setOutputPath(job, new Path(outputDir + "/identity_map"));
    try {
      job.waitForCompletion(true);
    } catch (ClassNotFoundException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
