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
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.identity.IdentifierType;

/**
 * The DataConfig class is a central repository for all configuration settings
 * across the system. Each configuration setting is represented in this class
 * (or one of its subclasses) as a field, which is populated from some
 * properties file stored on S3. The properties files are assumed to be
 * formatted as Java key/value files.
 *
 * The class can be used in two ways. During early phases in the processing run
 * a partial DataConfig object may be created. For example, during the initial
 * bootstrap lambda function for a data set it is not yet known what type of
 * infrastructure will be required, and so any settings regarding infrastructure
 * will be null. Later in the run, it is expected that all configuration
 * settings will be known, and so the complete config object may be created.
 * These two cases can be distinguished by whether or not the data config is
 * verified; an incomplete config object will not be verified, and so clients
 * should be aware that some values may be null, while a complete config will be
 * verified and should not contain null values.
 */
public class DataConfig {

  protected String paths;
  protected Map<String, String> rapidConfig;

  private final String gitTagOrBranch;
  private final String datasetName;
  private final String dataSource;
  private final IdentifierType mainIdentifier;
  private final String serverTimezone;
  private final FormatLibrary.Format pipelineFormat;
  private final FormatLibrary.Format fulltextFormat;

  private final String dataPipelineRole;
  private final String dataPipelineResourceRoleArn;
  private final String dataPipelineCreatorRoleArn;
  private final String keypair;
  private final String subnetId;

  private final String datasetsDynamoTable;
  private final String pipelineDynamoTable;
  private final String leaseDynamoTable;
  private final String identityLease;
  private final Integer identityLeaseLengthSeconds;

  private final String logBucket;
  private final String codeBucket;
  private final String workingBucket;
  private final String reportBucket;
  private final String archiveBucket;
  private final String fullTextBucket;
  private final String scratchDir;
  private final String archivePath;
  private final String hdtMonitorUrl;

  private final String redshiftCluster;
  private final String redshiftServer;
  private final String redshiftDatabase;
  private final String redshiftPort;
  private final String redshiftUserName;
  private final String redshiftPassword;

  private final String identityOraclePassword;
  private final String identityOraclePort;
  private final String identityOracleSchema;
  private final String identityOracleServer;
  private final String identityOracleSid;
  private final String identityOracleUserName;
  private final String identityOracleView;

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
  private final String emrConfiguration;

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
  private final String emrLogDir;
  private final String fullTextDir;
  private final String dataToolsJar;
  private final String identityRedshiftSchema;
  private final String identityRedshiftLoadScript;
  private final String redshiftUnloadScript;
  private final String fullTextScriptFile;
  private final String s3ToHdfsManifestFile;
  private final String redshiftLoadScript;
  private final String redshiftStagingDir;
  private final String hdfsBase;
  private final String hdfsVerifyBase;
  private final String rapidScriptFile;
  private final String rapidRuntime;
  private final String rapidConfigFile;
  private final String rapidRequestsDir;
  private final String rapidCodeDir;
  
 
  //RAPID Optional Configuration START
  private final String rapidGoogleCreds;
  private final String rapidGoogleDir;
  private final String rapidOutputDir;
  private final String rapidAwsDefaultRegion;
  private final String rapidGithubTokenName;
  private final String rapidGithubBranch;
  private final String rapidGithubUrl;
  private final String rapidGithubRequestUrl;
  private final String rapidGithubRequestBranch;
  private final String rapidRsConfigKey;
  private final String rapidRsConfigRegion;
  private final String rapidRsConfigBucket;
  private final String rapidCanvasConfigKey;
  private final String rapidCanvasConfigRegion;
  private final String rapidCanvasConfigBucket;
  private final String rapidAwsDefaultAccessKeyUsername;
  private final String rapidAwsDefaultAccessSecretKey;
  private final String rapidAwsAssumeRoleArn;
  private final String rapidAwsAssumeRoleSessionName;
  //RAPID Optional Configuration END

  protected String codeGeneratorScript;
  protected String phase0Class;
  protected String codeManagerClass;

  private final Properties properties;

  public DataConfig(final List<? extends InputStream> streams, final boolean verify)
      throws IOException, DataConfigurationException {
    properties = new Properties();
    for (final InputStream in : streams) {
      properties.load(in);
    }
    this.dataToolsJar = "data_tools.jar";
    this.identityRedshiftLoadScript = "s3_to_redshift_identity_loader.sql";
    this.redshiftUnloadScript = "redshift_unload.sql";
    this.redshiftLoadScript = "s3_to_redshift_loader.sql";
    this.s3ToHdfsManifestFile = "s3_to_hdfs_manifest.gz";
    this.fullTextScriptFile = "full_text_copy.sh";
    this.rapidScriptFile = "phase_3_rapid.sh";
    this.rapidRuntime = "/runtime/main.py";
    this.rapidConfigFile = "rapid_config.py";
    this.rapidCodeDir = "/home/hadoop/rapid-code";
    this.rapidRequestsDir = "/home/hadoop/rapid-requests";
    this.redshiftStagingDir = "redshift_staging";
    this.identityRedshiftSchema = "pii";
    this.emrCodeDir = "/home/hadoop/code";
    this.emrLogDir = "/home/hadoop";
    this.fullTextDir = "/tmp/full_text";
    this.hdfsBase = "/phase_";
    this.hdfsVerifyBase = "/verify" + this.hdfsBase;
    this.ec2GitDir = "/home/ec2-user/harvard-data-tools";
    this.ec2CodeDir = "/home/ec2-user/code";
    this.phase0Class = Phase0.class.getCanonicalName();

    this.scratchDir = getConfigParameter("scratch_dir", verify);
    this.redshiftPort = getConfigParameter("redshift_port", verify);
    this.awsKeyId = getConfigParameter("aws_key_id", verify);
    this.awsSecretKey = getConfigParameter("aws_secret_key", verify);

    this.dataSource = getConfigParameter("data_source", verify);
    this.datasetName = getConfigParameter("dataset_name", verify);
    this.pipelineFormat = Format.fromLabel(getConfigParameter("pipeline_format", verify));
    this.fulltextFormat = Format.fromLabel(getConfigParameter("fulltext_format", verify));
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
    this.fullTextBucket = getConfigParameter("full_text_bucket", verify);
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
    this.hdtMonitorUrl = getConfigParameter("hdt_monitor_url", verify);

    this.identityOraclePassword = getConfigParameter("identity_oracle_password", verify);
    this.identityOraclePort = getConfigParameter("identity_oracle_port", verify);
    this.identityOracleSchema = getConfigParameter("identity_oracle_schema", verify);
    this.identityOracleServer = getConfigParameter("identity_oracle_server", verify);
    this.identityOracleSid = getConfigParameter("identity_oracle_sid", verify);
    this.identityOracleUserName = getConfigParameter("identity_oracle_user_name", verify);
    this.identityOracleView = getConfigParameter("identity_oracle_view", verify);

    this.datasetsDynamoTable = getConfigParameter("datasets_dynamo_table", verify);
    this.leaseDynamoTable = getConfigParameter("lease_dynamo_table", verify);
    this.identityLease = getConfigParameter("identity_lease", verify);
    this.identityLeaseLengthSeconds = getIntConfigParameter("identity_lease_length_seconds",
        verify);

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
    this.emrConfiguration = getConfigParameter("emr_configuration", verify);

    this.phase0InstanceType = getConfigParameter("phase_0_instance_type", verify);
    this.phase0BidPrice = getConfigParameter("phase_0_bid_price", verify);
    this.phase0TerminateAfter = getConfigParameter("phase_0_terminate_after", verify);
    this.phase0Threads = getConfigParameter("phase_0_threads", verify);
    this.phase0HeapSize = getConfigParameter("phase_0_heap_size", verify);
    this.phase0Ami = getConfigParameter("phase_0_ami", verify);
    this.phase0SecurityGroup = getConfigParameter("phase_0_security_group", verify);
    this.phase0AvailabilityZoneGroup = getConfigParameter("phase_0_availability_zone_group",
        verify);
    
    //RAPID Optional Configuration START
    this.rapidGoogleCreds = getConfigParameter("rapid_google_creds", false);
    this.rapidGoogleDir = getConfigParameter("rapid_google_dir", false);
    this.rapidOutputDir = getConfigParameter("rapid_output_dir", false);
    this.rapidAwsDefaultRegion = getConfigParameter("rapid_aws_default_region", false);
    this.rapidGithubTokenName = getConfigParameter("rapid_github_token_name", false);
    this.rapidGithubBranch = getConfigParameter("rapid_github_branch", false);
    this.rapidGithubUrl = getConfigParameter("rapid_github_url", false);
    this.rapidGithubRequestUrl = getConfigParameter("rapid_github_request_url", false);
    this.rapidGithubRequestBranch = getConfigParameter("rapid_github_request_branch", false);
    this.rapidRsConfigKey = getConfigParameter("rapid_rs_config_key", false);
    this.rapidRsConfigRegion = getConfigParameter("rapid_rs_config_region", false);
    this.rapidRsConfigBucket = getConfigParameter("rapid_rs_config_bucket", false);
    this.rapidCanvasConfigKey = getConfigParameter("rapid_canvas_config_key", false);
    this.rapidCanvasConfigRegion = getConfigParameter("rapid_canvas_config_region", false);
    this.rapidCanvasConfigBucket = getConfigParameter("rapid_canvas_config_bucket", false);
    this.rapidAwsDefaultAccessKeyUsername = getConfigParameter("rapid_aws_default_access_key_username", false);
    this.rapidAwsDefaultAccessSecretKey = getConfigParameter("rapid_aws_default_access_secrect_key", false);
    this.rapidAwsAssumeRoleArn = getConfigParameter("rapid_aws_assume_role_arn", false);
    this.rapidAwsAssumeRoleSessionName = getConfigParameter("rapid_aws_assume_role_session_name", false);
    //RAPID Optional Configuration END
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

  public static <T extends DataConfig> T parseInputFiles(final Class<T> cls,
	      final String configPathString, final boolean verify, final Map<String, String> rapidConfig)
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
	      config.rapidConfig = rapidConfig;
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
  

  protected Integer getIntConfigParameter(final String key, final boolean verify)
      throws DataConfigurationException {
    final String tmp = getConfigParameter(key, verify);
    if (tmp == null) {
      return null;
    }
    return Integer.parseInt(tmp);
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
    checkParameter("codeManagerClass", codeManagerClass);
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

  public S3ObjectId getFullTextLocation() {
    return AwsUtils.key(fullTextBucket, datasetName);
  }

  public S3ObjectId getPhase0BootstrapScript() {
    return AwsUtils.key(getCodeLocation(), "phase-0-bootstrap.sh");
  }

  public S3ObjectId getIndexFileS3Location(final String runId) {
    return AwsUtils.key(getS3WorkingLocation(runId), "directoryList.json");
  }

  public S3ObjectId getMavenRepoCacheS3Location() {
    return AwsUtils.key(codeBucket, "maven_cache.tgz");
  }

  public String getHdfsDir(final int phase) {
    return hdfsBase + phase;
  }

  public String getPhase0IdMapPath() {
    return getHdfsDir(0) + "/identity_map";
  }

  public String getPhase1IdMapPath() {
    return getHdfsDir(1) + "/identity_map";
  }

  public String getPhase1TempIdMapOutput() {
    return "tempidentitymap";
  }

  public String getPhase1IdMapOutput() {
    return "identitymap/identitymap";
  }

  public String getPhase1EmailOutput() {
    return IdentifierType.EmailAddress.getFieldName();
  }
  
  public String getPhase1EmailPrimaryOutput() {
    return IdentifierType.EmailAddressPrimary.getFieldName().replace("_","");
  }

  public String getPhase1NameOutput() {
    return IdentifierType.Name.getFieldName();
  }
  
  public String getPhase1NameFirstOutput() {
    return IdentifierType.NameFirst.getFieldName().replace("_","");
  }
  
  public String getPhase1NameLastOutput() {
    return IdentifierType.NameLast.getFieldName().replace("_","");
  }

  public String getCreateHiveTables(final int phase) {
    return "phase_" + phase + "_create_tables.sh";
  }

  public String getVerifyHdfsDir(final int phase) {
    return hdfsVerifyBase + phase;
  }

  public String getFullTextDir() {
    return fullTextDir;
  }

  public String getMoveUnmodifiedScript(final int phase) {
    return "phase_" + phase + "_move_unmodified_files.sh";
  }

  public String getEmrLogFile(final String stepId) {
    return emrLogDir + "/" + stepId + ".out";
  }
  
  public String getEmrLogDir() {
	return emrLogDir;
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

  public String getFullTextScriptFile() {
    return fullTextScriptFile;
  }

  public String getS3ToHdfsManifestFile() {
    return s3ToHdfsManifestFile;
  }

  public String getRedshiftStagingDir() {
    return redshiftStagingDir;
  }
  
  public String getRapidScriptFile() {
	return rapidScriptFile;
  }
  
  public String getRapidRuntime() {
	return rapidRuntime;
	  
  }
  
  public String getRapidConfigFile() {
	return rapidConfigFile;
  }
  
  public Map<String, String> getRapidConfig() {
	return rapidConfig;
  }

  public String getRapidCodeDir() {
    return rapidCodeDir;
  }
  
  public String getRapidRequestsDir() {
	return rapidRequestsDir;
  }
  
  public String getRapidGoogleCreds() {
	return rapidGoogleCreds;
  }
  
  public String getRapidGoogleDir() {
	return rapidGoogleDir;
  }
  
  public String getRapidOutputDir() {
	return rapidOutputDir;
  }
  
  public String getRapidAwsDefaultRegion() {
	return rapidAwsDefaultRegion;
  }

  public String getRapidGithubTokenName() {
	return rapidGithubTokenName;
  }

  public String rapidGithubBranch() {
	return rapidGithubBranch;
  }

  public String getRapidGithubBranch() {
	return rapidGithubBranch;
  }

  public String getRapidGithubUrl() {
	return rapidGithubUrl;
  }

  public String getRapidGithubRequestUrl() {
	return rapidGithubRequestUrl;
  }

  public String getRapidGithubRequestBranch() {
	return rapidGithubRequestBranch;
  }

  public String getRapidRsConfigKey() {
	return rapidRsConfigKey;
  }
  
  public String getRapidRsConfigRegion() {
	return rapidRsConfigRegion;
  }

  public String getRapidRsConfigBucket() {
	return rapidRsConfigBucket;
  }
  
  public String getRapidCanvasConfigKey() {
	return rapidCanvasConfigKey;
  }
  
  public String getRapidCanvasConfigRegion() {
	return rapidCanvasConfigRegion;
  }
  
  public String getRapidCanvasConfigBucket() {
	return rapidCanvasConfigBucket;
  }
  
  public String getRapidAwsDefaultAccessKeyUsername() {
	return rapidAwsDefaultAccessKeyUsername;
  }
  
  public String getRapidAwsDefaultAccessSecretKey() {
	return rapidAwsDefaultAccessSecretKey;
  }
  
  public String getRapidAwsAssumeRoleArn() {
	return rapidAwsAssumeRoleArn;
  }
  
  public String getRapidAwsAssumeRoleSessionName() {
    return rapidAwsAssumeRoleSessionName;
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

  public String getCodeManagerClass() {
    return codeManagerClass;
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

  public String getEmrConfiguration() {
	  return emrConfiguration;
  }
  
  public String getLeaseDynamoTable() {
    return leaseDynamoTable;
  }

  public String getIdentityLease() {
    return identityLease;
  }

  public int getIdentityLeaseLengthSeconds() {
    return identityLeaseLengthSeconds;
  }

  public FormatLibrary.Format getPipelineFormat() {
    return pipelineFormat;
  }

  public FormatLibrary.Format getFulltextFormat() {
	    return fulltextFormat;
  }  

  public String getHdtMonitorUrl() {
    return hdtMonitorUrl;
  }

  public String getDatasetsDynamoTable() {
    return datasetsDynamoTable;
  }

  public String getIdentityOracleServer() {
    return identityOracleServer;
  }

  public String getIdentityOraclePort() {
    return identityOraclePort;
  }

  public String getIdentityOracleSid() {
    return identityOracleSid;
  }

  public String getIdentityOracleUserName() {
    return identityOracleUserName;
  }

  public String getIdentityOraclePassword() {
    return identityOraclePassword;
  }

  public String getIdentityOracleUrl() {
    return "jdbc:oracle:thin:@" + identityOracleServer + ":" + identityOraclePort + ":"
        + identityOracleSid;
  }

  public String getIdentityOracleSchema() {
    return identityOracleSchema;
  }

  public String getIdentityOracleView() {
    return identityOracleView;
  }

  public String getRedshiftUnloadScript() {
    return redshiftUnloadScript;
  }

}
