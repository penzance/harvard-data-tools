package edu.harvard.data.identity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.CodeManager;
import edu.harvard.data.HadoopUtilities;
import edu.harvard.data.leases.LeaseRenewalException;
import edu.harvard.data.leases.LeaseRenewalThread;
import edu.harvard.data.pipeline.InputTableIndex;

public class IdentityMapHadoopJob {
  private static final Logger log = LogManager.getLogger();

  private final DataConfig config;
  private final String inputDir;
  private final String outputDir;
  private final String runId;
  private final Configuration hadoopConfig;
  final HadoopUtilities hadoopUtils;
  private final InputTableIndex dataIndex;
  private final CodeManager codeManager;

  @SuppressWarnings("unchecked")
  public static void main(final String[] args)
      throws IOException, DataConfigurationException, LeaseRenewalException, ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    final String configPathString = args[0];
    final String runId = args[1];
    final String codeManagerClassName = args[2];

    final Class<? extends CodeManager> codeManagerClass = (Class<? extends CodeManager>) Class
        .forName(codeManagerClassName);
    final CodeManager codeManager = codeManagerClass.newInstance();

    final AwsUtils aws = new AwsUtils();
    final DataConfig config = codeManager.getDataConfig(configPathString, true);
    final S3ObjectId indexLocation = config.getIndexFileS3Location(runId);
    final InputTableIndex dataIndex = InputTableIndex.read(aws, indexLocation);
    new IdentityMapHadoopJob(config, codeManager, dataIndex, runId).run();
  }

  public IdentityMapHadoopJob(final DataConfig config, final CodeManager codeManager,
      final InputTableIndex dataIndex, final String runId) throws IOException {
    this.config = config;
    this.codeManager = codeManager;
    this.dataIndex = dataIndex;
    this.runId = runId;
    this.inputDir = config.getHdfsDir(0);
    this.outputDir = config.getHdfsDir(1);
    this.hadoopConfig = new Configuration();
    this.hadoopUtils = new HadoopUtilities();
  }

  @SuppressWarnings("rawtypes")
  protected void run() throws IOException, LeaseRenewalException {
    final LeaseRenewalThread leaseThread = LeaseRenewalThread.setup(config.getLeaseDynamoTable(),
        config.getIdentityLease(), runId, config.getIdentityLeaseLengthSeconds());
    final IdentifierType mainIdentifier = config.getMainIdentifier();
    hadoopConfig.set("format", config.getPipelineFormat().toString());
    hadoopConfig.set("mainIdentifier", mainIdentifier.toString());
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

    final Map<String, Class<? extends Mapper<Object, Text, ?, HadoopIdentityKey>>> allMapperClasses = codeManager
        .getIdentityMapperClasses();
    final Map<String, Class<? extends Mapper>> mapperClasses = new HashMap<String, Class<? extends Mapper>>();
    for (final String table : allMapperClasses.keySet()) {
      if (dataIndex.containsTable(table)) {
        mapperClasses.put(table, allMapperClasses.get(table));
      }
    }
    for (final String table : mapperClasses.keySet()) {
      final Path path = new Path(inputDir + "/" + table + "/");
      MultipleInputs.addInputPath(job, path, TextInputFormat.class, mapperClasses.get(table));
      log.info("Adding mapper for path " + path);
    }
    FileOutputFormat.setOutputPath(job, new Path(outputDir + "/identity_map"));
    try {
      job.waitForCompletion(true);
    } catch (ClassNotFoundException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    leaseThread.checkLease();
  }
}
