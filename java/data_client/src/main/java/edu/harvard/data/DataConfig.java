package edu.harvard.data;

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

import edu.harvard.data.identity.IdentifierType;

public class DataConfig {

  public String paths;

  private final String datasetName;
  private final String dataSource;
  private final String dataPipelineRole;
  private final String dataPipelineResourceRoleArn;
  private final String keypair;
  private final String subnetId;
  private final String emrReleaseLabel;
  private final String emrTerminateAfter;
  private final String emrMasterInstanceType;
  private final String emrMasterBidPrice;
  private final String emrCoreInstanceCount;
  private final String emrCoreInstanceType;
  private final String emrCoreBidPrice;
  private final String emrTaskInstanceCount;
  private final String emrTaskInstanceType;
  private final String emrTaskBidPrice;
  private final String emrMaximumRetries;
  private final String gitTagOrBranch;
  private final String logBucket;
  private final String codeBucket;
  private final String incomingBucket;
  public final String workingBucket;
  public final String reportBucket;
  public final String redshiftCluster;
  public final String redshiftServer;
  public final String redshiftDatabase;
  public final String redshiftPort;
  public final String redshiftUserName;
  public final String redshiftPassword;
  public final String failureSnsArn;
  public final String successSnsArn;
  public final String completionSnsArn;
  public final String scratchDir;
  public final String awsKeyId;
  public final String awsSecretKey;
  public final IdentifierType mainIdentifier;
  public String pipelineDynamoTable;

  public final String emrCodeDir;
  public final String dataToolsJar;
  public final String identityRedshiftSchema;
  public final String identityRedshiftLoadScript;
  public final String redshiftLoadScript;
  public final String redshiftStagingDir;
  private final String hdfsBase;
  private final String hdfsVerifyBase;


  private final Properties properties;

  public DataConfig(final List<? extends InputStream> streams, final boolean verify)
      throws IOException, DataConfigurationException {
    properties = new Properties();
    for (final InputStream in : streams) {
      properties.load(in);
    }
    this.dataToolsJar = "data_tools.jar";
    this.identityRedshiftLoadScript = "s3_to_redshift_identity_loader.sql";
    this.redshiftLoadScript = "s3_to_redshift_loader.sql";
    this.redshiftStagingDir = "redshift_staging";
    this.identityRedshiftSchema = "pii";
    this.emrCodeDir = "/home/hadoop/code";
    this.hdfsBase = "/phase_";
    this.hdfsVerifyBase = "/verify" + this.hdfsBase;

    this.scratchDir = getConfigParameter("scratch_dir", verify);
    this.redshiftPort = getConfigParameter("redshift_port", verify);
    this.incomingBucket = getConfigParameter("incoming_bucket", verify);
    this.awsKeyId = getConfigParameter("aws_key_id", verify);
    this.awsSecretKey = getConfigParameter("aws_secret_key", verify);

    this.dataSource = getConfigParameter("data_source", verify);
    this.datasetName = getConfigParameter("dataset_name", verify);
    this.dataPipelineRole = getConfigParameter("data_pipeline_role", verify);
    this.dataPipelineResourceRoleArn = getConfigParameter("data_pipeline_resource_role_arn",
        verify);
    this.keypair = getConfigParameter("keypair", verify);
    this.subnetId = getConfigParameter("subnet_id", verify);
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
    this.workingBucket = getConfigParameter("working_bucket", verify);
    this.reportBucket = getConfigParameter("report_bucket", verify);
    this.redshiftCluster = getConfigParameter("redshift_cluster", verify);
    this.redshiftServer = getConfigParameter("redshift_server", verify);
    this.redshiftDatabase = getConfigParameter("redshift_database", verify);
    this.redshiftUserName = getConfigParameter("redshift_user_name", verify);
    this.redshiftPassword = getConfigParameter("redshift_password", verify);
    this.failureSnsArn = getConfigParameter("failure_sns_arn", verify);
    this.successSnsArn = getConfigParameter("success_sns_arn", verify);
    this.completionSnsArn = getConfigParameter("completion_sns_arn", verify);
    this.pipelineDynamoTable = getConfigParameter("pipeline_dynamo_table", verify);
    this.mainIdentifier = IdentifierType.valueOf(getConfigParameter("main_identifier", verify));
    this.emrMaximumRetries = getConfigParameter("emr_maximum_retries", verify);
  }

  public static <T extends DataConfig> T parseInputFiles(final Class<T> cls,
      final String configPathString, final boolean verify)
          throws IOException, DataConfigurationException {
    final AwsUtils aws = new AwsUtils();
    final List<InputStream> streams = new ArrayList<InputStream>();
    try {
      for (final String file : configPathString.split("\\|")) {
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
    return "jdbc:postgresql://" + redshiftServer + ":" + redshiftPort + "/" + redshiftDatabase;
  }

  public S3ObjectId getS3WorkingLocation() {
    return AwsUtils.key(workingBucket, dataSource);
  }

  public S3ObjectId getS3IncomingLocation() {
    return AwsUtils.key(incomingBucket, dataSource);
  }

  public String getHdfsDir(final int phase) {
    return hdfsBase + phase;
  }

  public String getVerifyHdfsDir(final int phase) {
    return hdfsVerifyBase + phase;
  }

  public String getMoveUnmodifiedScript(final int phase) {
    return "phase_" + phase + "_move_unmodified_files.sh";
  }

  public String getDatasetName() {
    return datasetName;
  }

  public String getDataSource() {
    return dataSource;
  }

  public String getDataPipelineRole() {
    return dataPipelineRole;
  }

  public String getDataPipelineResourceRoleArn() {
    return dataPipelineResourceRoleArn;
  }

  public String getKeypair() {
    return keypair;
  }

  public String getSubnetId() {
    return subnetId;
  }

  public String getEmrReleaseLabel() {
    return emrReleaseLabel;
  }

  public String getEmrTerminateAfter() {
    return emrTerminateAfter;
  }

  public String getEmrMasterInstanceType() {
    return emrMasterInstanceType;
  }

  public String getEmrMasterBidPrice() {
    return emrMasterBidPrice;
  }

  public String getEmrCoreInstanceCount() {
    return emrCoreInstanceCount;
  }

  public String getEmrCoreInstanceType() {
    return emrCoreInstanceType;
  }

  public String getEmrCoreBidPrice() {
    return emrCoreBidPrice;
  }

  public String getEmrTaskInstanceCount() {
    return emrTaskInstanceCount;
  }

  public String getEmrTaskInstanceType() {
    return emrTaskInstanceType;
  }

  public String getEmrTaskBidPrice() {
    return emrTaskBidPrice;
  }

  public String getEmrMaximumRetries() {
    return emrMaximumRetries;
  }

  public String getGitTagOrBranch() {
    return gitTagOrBranch;
  }

  public String getLogBucket() {
    return logBucket;
  }

  public String getCodeBucket() {
    return codeBucket;
  }

  public String getIncomingBucket() {
    return incomingBucket;
  }

}
