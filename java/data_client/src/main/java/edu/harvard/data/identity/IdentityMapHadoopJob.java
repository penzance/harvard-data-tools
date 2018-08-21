package edu.harvard.data.identity;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.CodeManager;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.HadoopUtilities;
import edu.harvard.data.TableFormat;
import edu.harvard.data.leases.LeaseRenewalException;
import edu.harvard.data.leases.LeaseRenewalThread;
import edu.harvard.data.pipeline.InputTableIndex;

public class IdentityMapHadoopJob {
  private static final Logger log = LogManager.getLogger();

  private final DataConfig config;
  private final String inputDir;
  private final String runId;
  private final Configuration hadoopConfig;
  final HadoopUtilities hadoopUtils;
  private final InputTableIndex dataIndex;
  private final CodeManager codeManager;

  public static void main(final String[] args)
      throws IOException, DataConfigurationException, LeaseRenewalException, ClassNotFoundException,
      InstantiationException, IllegalAccessException, SQLException {
    final String configPathString = args[0];
    final String runId = args[1];
    final String codeManagerClassName = args[2];

    final CodeManager codeManager = CodeManager.getCodeManager(codeManagerClassName);
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
    this.hadoopConfig = new Configuration();
    this.hadoopUtils = new HadoopUtilities();
  }

  protected void run()
      throws IOException, LeaseRenewalException, SQLException, DataConfigurationException {
    final LeaseRenewalThread leaseThread = LeaseRenewalThread.setup(config.getLeaseDynamoTable(),
        config.getIdentityLease(), runId, config.getIdentityLeaseLengthSeconds());
    final IdentifierType mainIdentifier = config.getMainIdentifier();
    hadoopConfig.set("format", config.getPipelineFormat().toString() );
    hadoopConfig.set("mainIdentifier", mainIdentifier.toString());

    final Job job = getIdentityMapJob(config);
    addInitialIdentityMapPaths(config, job);
    configureMapperClasses(job);

    try {
      job.waitForCompletion(true);
    } catch (ClassNotFoundException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    lookupEppnAndHuid(mainIdentifier);
    leaseThread.checkLease();
  }

  private void lookupEppnAndHuid(final IdentifierType mainIdentifier)
      throws SQLException, DataConfigurationException, IOException {
    log.info("Looking up any unknown EPPNs or HUIDs");
    final TableFormat format = new FormatLibrary().getFormat(config.getPipelineFormat());
    log.info("Main Identifier is " + mainIdentifier);
    final HuidEppnLookup lookup = new HuidEppnLookup(config, format, mainIdentifier);

    final String idMapDir = config.getPhase1IdMapPath() + "/";

    final URI[] latest = getInputUris(idMapDir + config.getPhase1TempIdMapOutput());
    final URI[] original = getInputUris(config.getPhase0IdMapPath());
    final Path outputPath = new Path(idMapDir + config.getPhase1IdMapOutput());

    lookup.expandIdentities(latest, original, outputPath.toUri(), mainIdentifier.getType());
  }

  private URI[] getInputUris(final String path) throws IOException {
    final Path inputDirPath = new Path(path);
    final List<URI> inputUris = new ArrayList<URI>();
    for (final Path inputPath : hadoopUtils.listHdfsFiles(hadoopConfig, inputDirPath)) {
      inputUris.add(inputPath.toUri());
    }
    return inputUris.toArray(new URI[] {});
  }

  @SuppressWarnings("rawtypes")
  private void configureMapperClasses(final Job job) {
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
    FileOutputFormat.setOutputPath(job, new Path(config.getPhase1IdMapPath()));
  }

  private void addInitialIdentityMapPaths(final DataConfig config2, final Job job)
      throws IllegalArgumentException, IOException {
    for (final Path path : hadoopUtils.listHdfsFiles(hadoopConfig,
        new Path(config.getPhase0IdMapPath()))) {
      log.info("Adding identity file " + path + " to map job cache");
      job.addCacheFile(path.toUri());
    }
  }

  private Job getIdentityMapJob(final DataConfig config) throws IOException {
    final IdentifierType mainIdentifier = config.getMainIdentifier();
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

    MultipleOutputs.addNamedOutput(job, config.getPhase1TempIdMapOutput(), TextOutputFormat.class,
        Text.class, NullWritable.class);
    MultipleOutputs.addNamedOutput(job, config.getPhase1EmailOutput(), TextOutputFormat.class,
        Text.class, NullWritable.class);
    MultipleOutputs.addNamedOutput(job, config.getPhase1NameOutput(), TextOutputFormat.class,
        Text.class, NullWritable.class);
    
    return job;
  }
}
