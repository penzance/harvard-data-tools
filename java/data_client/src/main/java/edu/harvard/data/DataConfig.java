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

  protected String paths;

  private final String gitTagOrBranch;
  private final String datasetName;
  private final String dataSource;
  private final IdentifierType mainIdentifier;
  private final String serverTimezone;

  private final String dataPipelineRole;
  private final String dataPipelineResourceRoleArn;
  private final String dataPipelineCreatorRoleArn;
  private final String keypair;
  private final String subnetId;
  private final String pipelineDynamoTable;

  private final String logBucket;
  private final String codeBucket;
  private final String workingBucket;
  private final String reportBucket;
  private final String archiveBucket;
  private final String scratchDir;
  private final String archivePath;

  private final String redshiftCluster;
  private final String redshiftServer;
  private final String redshiftDatabase;
  private final String redshiftPort;
  private final String redshiftUserName;
  private final String redshiftPassword;

  private final String awsKeyId;
  private final String awsSecretKey;

  private final String failureSnsArn;
  private final String successSnsArn;
  private final String completionSnsArn;

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
  private final String emrAvailabilityZoneGroup;

  private final String phase0InstanceType;
  private final String phase0BidPrice;
  private final String phase0TerminateAfter;
  private final String phase0Threads;
  private final String phase0HeapSize;
  private final String phase0Ami;
  private final String phase0SecurityGroup;
  private final String phase0AvailabilityZoneGroup;

  private final String ec2GitDir;
  private final String ec2CodeDir;
  private final String emrCodeDir;
  private final String dataToolsJar;
  private final String identityRedshiftSchema;
  private final String identityRedshiftLoadScript;
  private final String redshiftLoadScript;
  private final String redshiftStagingDir;
  private final String hdfsBase;
  private final String hdfsVerifyBase;

  protected String codeGeneratorScript;
  protected String phase0Class;
  protected String pipelineSetupClass;

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
    this.ec2GitDir = "/home/ec2-user/harvard-data-tools";
    this.ec2CodeDir = "/home/ec2-user/code";

    this.scratchDir = getConfigParameter("scratch_dir", verify);
    this.redshiftPort = getConfigParameter("redshift_port", verify);
    this.awsKeyId = getConfigParameter("aws_key_id", verify);
    this.awsSecretKey = getConfigParameter("aws_secret_key", verify);

    this.dataSource = getConfigParameter("data_source", verify);
    this.datasetName = getConfigParameter("dataset_name", verify);
    this.dataPipelineRole = getConfigParameter("data_pipeline_role", verify);
    this.dataPipelineResourceRoleArn = getConfigParameter("data_pipeline_resource_role_arn",
        verify);
    this.dataPipelineCreatorRoleArn = getConfigParameter("data_pipeline_creator_role_arn", verify);
    this.keypair = getConfigParameter("keypair", verify);
    this.subnetId = getConfigParameter("subnet_id", verify);
    this.serverTimezone = getConfigParameter("server_timezone", verify);
    this.gitTagOrBranch = getConfigParameter("git_tag_or_branch", verify);
    this.logBucket = getConfigParameter("log_bucket", verify);
    this.codeBucket = getConfigParameter("code_bucket", verify);
    this.archiveBucket = getConfigParameter("archive_bucket", verify);
    this.archivePath = getConfigParameter("archive_path", verify);
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
    this.emrAvailabilityZoneGroup = getConfigParameter("emr_availability_zone_group", verify);

    this.phase0InstanceType = getConfigParameter("phase_0_instance_type", verify);
    this.phase0BidPrice = getConfigParameter("phase_0_bid_price", verify);
    this.phase0TerminateAfter = getConfigParameter("phase_0_terminate_after", verify);
    this.phase0Threads = getConfigParameter("phase_0_threads", verify);
    this.phase0HeapSize = getConfigParameter("phase_0_heap_size", verify);
    this.phase0Ami = getConfigParameter("phase_0_ami", verify);
    this.phase0SecurityGroup = getConfigParameter("phase_0_security_group", verify);
    this.phase0AvailabilityZoneGroup = getConfigParameter("phase_0_availability_zone_group", verify);
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

  protected void checkParameters() throws DataConfigurationException {
    checkParameter("codeGeneratorScript", codeGeneratorScript);
    checkParameter("phase0Class", phase0Class);
    checkParameter("pipelineSetupClass", pipelineSetupClass);
  }

  private void checkParameter(final String key, final String value)
      throws DataConfigurationException {
    if (value == null) {
      throw new DataConfigurationException("Configuration parameter " + key + " not set.");
    }
  }

  public String getRedshiftUrl() {
    return "jdbc:postgresql://" + redshiftServer + ":" + redshiftPort + "/" + redshiftDatabase;
  }

  public S3ObjectId getS3WorkingLocation(final String runId) {
    return AwsUtils.key(workingBucket, dataSource, runId);
  }

  public S3ObjectId getCodeLocation() {
    return AwsUtils.key(codeBucket, gitTagOrBranch);
  }

  public S3ObjectId getArchiveLocation() {
    return AwsUtils.key(archiveBucket, archivePath);
  }

  public S3ObjectId getPhase0BootstrapScript() {
    return AwsUtils.key(getCodeLocation(), "phase-0-bootstrap.sh");
  }

  public S3ObjectId getIndexFileS3Location(final String runId) {
    return AwsUtils.key(getS3WorkingLocation(runId), "directoryList.json");
  }

  public S3ObjectId getMavenRepoCacheS3Location() {
    return AwsUtils.key(getCodeLocation(), "maven_cache.tgz");
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

  public String getReportBucket() {
    return reportBucket;
  }

  public String getRedshiftCluster() {
    return redshiftCluster;
  }

  public String getRedshiftServer() {
    return redshiftServer;
  }

  public String getRedshiftDatabase() {
    return redshiftDatabase;
  }

  public String getRedshiftPort() {
    return redshiftPort;
  }

  public String getRedshiftUserName() {
    return redshiftUserName;
  }

  public String getRedshiftPassword() {
    return redshiftPassword;
  }

  public String getFailureSnsArn() {
    return failureSnsArn;
  }

  public String getSuccessSnsArn() {
    return successSnsArn;
  }

  public String getCompletionSnsArn() {
    return completionSnsArn;
  }

  public String getScratchDir() {
    return scratchDir;
  }

  public String getAwsKeyId() {
    return awsKeyId;
  }

  public String getAwsSecretKey() {
    return awsSecretKey;
  }

  public IdentifierType getMainIdentifier() {
    return mainIdentifier;
  }

  public String getPipelineDynamoTable() {
    return pipelineDynamoTable;
  }

  public String getEmrCodeDir() {
    return emrCodeDir;
  }

  public String getDataToolsJar() {
    return dataToolsJar;
  }

  public String getIdentityRedshiftSchema() {
    return identityRedshiftSchema;
  }

  public String getIdentityRedshiftLoadScript() {
    return identityRedshiftLoadScript;
  }

  public String getRedshiftLoadScript() {
    return redshiftLoadScript;
  }

  public String getRedshiftStagingDir() {
    return redshiftStagingDir;
  }

  public String getPaths() {
    return paths;
  }

  public void setPaths(final String paths) {
    this.paths = paths;
  }

  public String getPhase0InstanceType() {
    return phase0InstanceType;
  }

  public String getPhase0BidPrice() {
    return phase0BidPrice;
  }

  public String getPhase0TerminateAfter() {
    return phase0TerminateAfter;
  }

  public String getPhase0Threads() {
    return phase0Threads;
  }

  public String getPhase0HeapSize() {
    return phase0HeapSize;
  }

  public String getPhase0Ami() {
    return phase0Ami;
  }

  public String getPhase0SecurityGroup() {
    return phase0SecurityGroup;
  }

  public String getPhase0AvailabilityZoneGroup() {
    return phase0AvailabilityZoneGroup;
  }

  public String getEc2GitDir() {
    return ec2GitDir;
  }

  public String getCodeGeneratorScript() {
    return codeGeneratorScript;
  }

  public String getEc2CodeDir() {
    return ec2CodeDir;
  }

  public String getPhase0Class() {
    return phase0Class;
  }

  public String getPipelineSetupClass() {
    return pipelineSetupClass;
  }

  public String getDataPipelineCreatorRoleArn() {
    return dataPipelineCreatorRoleArn;
  }

  public String getServerTimezone() {
    return serverTimezone;
  }

  public String getEmrAvailabilityZoneGroup() {
    return emrAvailabilityZoneGroup;
  }

}
