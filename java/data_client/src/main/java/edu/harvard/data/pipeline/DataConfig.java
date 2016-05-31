package edu.harvard.data.pipeline;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;

public class DataConfig {

  public String paths;

  public final String dataPipelineRole;
  public final String dataPipelineResourceRoleArn;
  public final String keypair;
  public final String appSubnet;
  public final String emrReleaseLabel;
  public final String emrTerminateAfter;
  public final String emrMasterInstanceType;
  public final String emrMasterBidPrice;
  public final String emrCoreInstanceCount;
  public final String emrCoreInstanceType;
  public final String emrCoreBidPrice;
  public final String emrTaskInstanceCount;
  public final String emrTaskInstanceType;
  public final String emrTaskBidPrice;
  public final String gitTagOrBranch;
  public final String logBucket;
  public final String codeBucket;
  public final String intermediateBucket;
  public final String incomingBucket;
  public final String workingBucket;
  public final String redshiftCluster;
  public final String redshiftDatabase;
  public final String redshiftPort;
  public final String redshiftUserName;
  public final String redshiftPassword;
  public final String redshiftAccessKey;
  public final String redshiftAccessSecret;
  public final String failureSnsArn;
  public final String successSnsArn;
  public final String scratchDir;
  public final String awsKeyId;
  public final String awsSecretKey;

  // XXX To remove...
  public final String lowercaseDatasource;
  public final String dataSourceSchemaVersion;

  private final Properties properties;

  protected DataConfig(final List<? extends InputStream> streams, final boolean verify)
      throws IOException, DataConfigurationException {
    properties = new Properties();
    for (final InputStream in : streams) {
      properties.load(in);
    }

    this.scratchDir = getConfigParameter("scratch_dir", verify);
    this.redshiftPort = getConfigParameter("redshift_port", verify);
    this.incomingBucket = getConfigParameter("incoming_bucket", verify);
    this.awsKeyId = getConfigParameter("aws_key_id", verify);
    this.awsSecretKey = getConfigParameter("aws_secret_key", verify);

    this.dataPipelineRole = getConfigParameter("data_pipeline_role", verify);
    this.dataPipelineResourceRoleArn = getConfigParameter("data_pipeline_resource_role_arn",
        verify);
    this.keypair = getConfigParameter("keypair", verify);
    this.appSubnet = getConfigParameter("app_subnet", verify);
    this.emrReleaseLabel = getConfigParameter("emr_release_label", verify);
    this.emrTerminateAfter = getConfigParameter("emr_terminate_after", verify);
    this.emrMasterInstanceType = getConfigParameter("emr_master_instance_type", verify);
    this.emrCoreInstanceType = getConfigParameter("emr_core_instance_type", verify);
    this.emrTaskInstanceType = getConfigParameter("emr_task_instance_type", verify);
    this.emrMasterBidPrice = getConfigParameter("emr_master_bid_price", false);
    this.emrCoreBidPrice = getConfigParameter("emr_core_bid_price", false);
    this.emrTaskBidPrice = getConfigParameter("emr_task_bid_price", false);
    this.emrCoreInstanceCount = getConfigParameter("emr_core_instance_count", verify);
    this.emrTaskInstanceCount = getConfigParameter("emr_task_instance_count", verify);
    this.gitTagOrBranch = getConfigParameter("git_tag_or_branch", verify);
    this.logBucket = getConfigParameter("log_bucket", verify);
    this.codeBucket = getConfigParameter("code_bucket", verify);
    this.intermediateBucket = getConfigParameter("intermediate_bucket", verify);
    this.workingBucket = getConfigParameter("working_bucket", verify);
    this.redshiftCluster = getConfigParameter("redshift_cluster", verify);
    this.redshiftDatabase = getConfigParameter("redshift_database", verify);
    this.redshiftUserName = getConfigParameter("redshift_user_name", verify);
    this.redshiftPassword = getConfigParameter("redshift_password", verify);
    this.redshiftAccessKey = getConfigParameter("redshift_access_key", verify);
    this.redshiftAccessSecret = getConfigParameter("redshift_access_secret", verify);
    this.failureSnsArn = getConfigParameter("failure_sns_arn", verify);
    this.successSnsArn = getConfigParameter("success_sns_arn", verify);
    this.lowercaseDatasource = getConfigParameter("lowercase_data_source", verify);
    this.dataSourceSchemaVersion = getConfigParameter("data_source_schema_version", verify);
  }

  public static <T extends DataConfig> T parseInputFiles(final Class<T> cls,
      final String configPathString, final boolean verify)
          throws IOException, DataConfigurationException {
    final AwsUtils aws = new AwsUtils();
    final List<InputStream> streams = new ArrayList<InputStream>();
    try {
      for (final String file : configPathString.split("|")) {
        if (file.toLowerCase().startsWith("s3://")) {
          final S3ObjectId awsFile = AwsUtils.key(file);
          if (!aws.isFile(awsFile)) {
            throw new FileNotFoundException("Can't find configuration S3 key " + file);
          }
          streams.add(aws.getInputStream(awsFile, false));
        } else {
          final File f = new File(file);
          if (!f.exists() || f.isDirectory()) {
            throw new FileNotFoundException("Can't find configuration file " + f);
          }
          streams.add(new FileInputStream(file));
        }
      }
      final Constructor<T> constructor = cls.getConstructor(List.class, boolean.class);
      final T config = constructor.newInstance(streams, verify);
      config.paths = configPathString;
      return config;
    } catch (NoSuchMethodException | SecurityException | InstantiationException
        | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new DataConfigurationException(e);
    } finally {
      for (final InputStream in : streams) {
        in.close();
      }
    }
  }

  protected String getConfigParameter(final String key, final boolean verify)
      throws DataConfigurationException {
    final String param = (String) properties.get(key);
    if (verify && param == null) {
      throw new DataConfigurationException("Configuration file missing " + key);
    }
    return param;
  }

  public String getRedshiftUrl() {
    return "jdbc:postgresql://" + redshiftCluster + ":" + redshiftPort + "/" + redshiftDatabase;
  }

}
