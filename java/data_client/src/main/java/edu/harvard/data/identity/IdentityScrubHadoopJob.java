package edu.harvard.data.identity;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.CodeManager;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.HadoopUtilities;
import edu.harvard.data.NoInputDataException;
import edu.harvard.data.leases.LeaseRenewalException;
import edu.harvard.data.leases.LeaseRenewalThread;
import edu.harvard.data.pipeline.InputTableIndex;

public class IdentityScrubHadoopJob {

  private final DataConfig config;
  private final CodeManager codeManager;
  private final InputTableIndex dataIndex;
  private final URI hdfsService;
  private final HadoopUtilities hadoopUtils;
  private final String runId;

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  LeaseRenewalException, InstantiationException, IllegalAccessException, NoInputDataException,
  URISyntaxException, ClassNotFoundException, InterruptedException {
    final String configPathString = args[0];
    final String runId = args[1];
    final String codeManagerClassName = args[2];

    final CodeManager codeManager = CodeManager.getCodeManager(codeManagerClassName);
    final AwsUtils aws = new AwsUtils();
    final DataConfig config = codeManager.getDataConfig(configPathString, true);
    final S3ObjectId indexLocation = config.getIndexFileS3Location(runId);
    final InputTableIndex dataIndex = InputTableIndex.read(aws, indexLocation);

    new IdentityScrubHadoopJob(config, codeManager, dataIndex, runId).run();
  }

  public IdentityScrubHadoopJob(final DataConfig config, final CodeManager codeManager,
      final InputTableIndex dataIndex, final String runId) throws URISyntaxException {
    this.config = config;
    this.codeManager = codeManager;
    this.dataIndex = dataIndex;
    this.runId = runId;
    this.hdfsService = new URI("hdfs///");
    this.hadoopUtils = new HadoopUtilities();
  }

  protected void run() throws InstantiationException, IllegalAccessException, IOException,
  NoInputDataException, ClassNotFoundException, InterruptedException, LeaseRenewalException {
    final LeaseRenewalThread leaseThread = LeaseRenewalThread.setup(config.getLeaseDynamoTable(),
        config.getIdentityLease(), runId, config.getIdentityLeaseLengthSeconds());

    final Map<String, Class<? extends IdentityScrubber<?>>> scrubbers = codeManager
        .getIdentityScrubberClasses();
    final List<Job> jobs = new ArrayList<Job>();
    for (final String tableName : scrubbers.keySet()) {
      if (dataIndex.containsTable(tableName)) {
        final Class<? extends IdentityScrubber<?>> cls = scrubbers.get(tableName);
        jobs.add(getJob(tableName, cls));
      }
    }

    for (final Job job : jobs) {
      job.submit();
    }
    for (final Job job : jobs) {
      job.waitForCompletion(true);
    }
    leaseThread.checkLease();
  }

  private Job getJob(final String tableName, final Class<? extends IdentityScrubber<?>> cls)
      throws IOException, NoInputDataException {
    final Configuration hadoopConfig = new Configuration();
    hadoopConfig.set("format", Format.fromLabel( Format.DecompressedInternal.getLabel() ).toString() );
    hadoopConfig.set("config", config.getPaths());
    final Job job = Job.getInstance(hadoopConfig, tableName + "-scrubber");
    job.setJarByClass(IdentityScrubHadoopJob.class);
    job.setMapperClass(cls);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    hadoopUtils.setPaths(job, hdfsService, config.getHdfsDir(0) + "/" + tableName,
        config.getHdfsDir(1) + "/" + tableName);
    for (final Path path : hadoopUtils.listHdfsFiles(hadoopConfig,
        new Path(config.getHdfsDir(1) + "/identity_map/identitymap"))) {
      job.addCacheFile(path.toUri());
    }

    return job;
  }

}
