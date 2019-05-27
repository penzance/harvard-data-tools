package edu.harvard.data.pipeline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsRequest;
import com.amazonaws.services.ec2.model.DescribeSpotInstanceRequestsResult;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.NetworkInterface;
import com.amazonaws.services.ec2.model.InstanceNetworkInterfaceSpecification;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.util.Base64;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DatasetInfo;
import edu.harvard.data.pipeline.PipelineExecutionRecord.Status;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public abstract class Phase0Bootstrap {

  private static final Logger log = LogManager.getLogger();

  protected DataConfig config;
  private String configPathString;
  private Class<? extends DataConfig> configClass;
  protected String runId;
  private boolean createPipeline;
  protected AwsUtils aws;
  private PipelineExecutionRecord executionRecord;

  protected abstract List<S3ObjectId> getInfrastructureConfigPaths()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException;

  protected abstract Map<String, String> getCustomEc2Environment()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException;

  protected abstract boolean newDataAvailable()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException;

  protected abstract void setup()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException;

  protected void init(final String configPathString, final Class<? extends DataConfig> configClass,
      final boolean createPipeline) throws IOException, DataConfigurationException {
    log.info("Configuration path: " + configPathString);
    this.configPathString = configPathString;
    this.configClass = configClass;
    this.createPipeline = createPipeline;
    this.config = DataConfig.parseInputFiles(configClass, configPathString, false);
    this.runId = getRunId();
    this.aws = new AwsUtils();
  }

  protected void run(final Context context)
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    final String msg = "Details at " + config.getHdtMonitorUrl() + "/data_dashboard/pipeline/"
        + config.getGitTagOrBranch() + "/" + runId;
    sendSnsNotification("Run " + runId + " started.", msg, config.getSuccessSnsArn());

    setup();
    for (final S3ObjectId path : getInfrastructureConfigPaths()) {
      configPathString += "|" + AwsUtils.uri(path);
    }
    this.config = DataConfig.parseInputFiles(configClass, configPathString, false);

    PipelineExecutionRecord.init(config.getPipelineDynamoTable());
    DatasetInfo.init(config.getDatasetsDynamoTable());

    executionRecord = new PipelineExecutionRecord(runId);
    executionRecord.setBootstrapLogStream(context.getLogStreamName());
    executionRecord.setBootstrapLogGroup(context.getLogGroupName());
    executionRecord.setRunStart(new Date());
    executionRecord.setStatus(Status.ProvisioningPhase0.toString());
    executionRecord.setConfigString(configPathString);
    executionRecord.setWorkingDirectory(AwsUtils.uri(config.getS3WorkingLocation(runId)));
    executionRecord.save();

    final DatasetInfo dataset = new DatasetInfo(config.getDatasetName());
    dataset.setRunId(runId);
    dataset.save();

    if (newDataAvailable()) {
      createPhase0();
    }
  }

  private void createPhase0()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    final AmazonEC2Client ec2client = new AmazonEC2Client();

    final LaunchSpecification spec = new LaunchSpecification();
    log.info("Subnet ID: " + config.getSubnetId());
    spec.setImageId(config.getPhase0Ami());
    spec.setInstanceType(config.getPhase0InstanceType());
    spec.setKeyName(config.getKeypair());
    spec.setSubnetId(config.getSubnetId());
    spec.setUserData(getUserData(config));
    final IamInstanceProfileSpecification instanceProfile = new IamInstanceProfileSpecification();
    instanceProfile.setArn(config.getDataPipelineCreatorRoleArn());
    spec.setIamInstanceProfile(instanceProfile);
    log.info("Get Subnet ID: " + spec.getSubnetId());
    
    // set network interface
    InstanceNetworkInterfaceSpecification network = new InstanceNetworkInterfaceSpecification();
    network.setSubnetId(config.getSubnetId());
    final List<String> securityGroups = new ArrayList<>();
    securityGroups.add(config.getPhase0SecurityGroup());
    network.setGroups(securityGroups);
    final List<InstanceNetworkInterfaceSpecification> networkList = new ArrayList<>();
    spec.setNetworkInterfaces(networkList);
    log.info("Get Network Interfaces: " + spec.getNetworkInterfaces());

    final RequestSpotInstancesRequest request = new RequestSpotInstancesRequest();
    request.setSpotPrice(config.getPhase0BidPrice());
    // request.setAvailabilityZoneGroup(config.getPhase0AvailabilityZoneGroup());
    request.setInstanceCount(1);
    request.setLaunchSpecification(spec);
    log.info("Get Launch Spec Subnet ID: " + request.getLaunchSpecification().getSubnetId());

    final RequestSpotInstancesResult result = ec2client.requestSpotInstances(request);
    final String requestId = result.getSpotInstanceRequests().get(0).getSpotInstanceRequestId();
    System.out.println(result);
    executionRecord.setPhase0RequestId(requestId);
    executionRecord.save();

    final List<String> instanceIds = new ArrayList<String>();
    instanceIds.add(requestId);
    final DescribeSpotInstanceRequestsRequest describe = new DescribeSpotInstanceRequestsRequest();
    describe.setSpotInstanceRequestIds(instanceIds);
    final DescribeSpotInstanceRequestsResult description = ec2client
        .describeSpotInstanceRequests(describe);
    System.out.println(description);
  }

  protected void sendSnsNotification(final String subject, final String msg, final String arn) {
    final AmazonSNSClient sns = new AmazonSNSClient();
    final PublishRequest publishRequest = new PublishRequest(arn, msg, subject);
    sns.publish(publishRequest);
  }

  private String getUserData(final DataConfig config)
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    final S3ObjectId bootstrapScript = config.getPhase0BootstrapScript();

    String userData = "#! /bin/bash\n";
    userData += getBootstrapEnvironment(config);
    try (final BufferedReader in = new BufferedReader(
        new InputStreamReader(aws.getInputStream(bootstrapScript, false)))) {
      String line = in.readLine();
      while (line != null) {
        userData += line + "\n";
        line = in.readLine();
      }
    }
    log.info("User data: " + userData);
    return Base64.encodeAsString(userData.getBytes("UTF-8"));
  }

  private String getBootstrapEnvironment(final DataConfig config)
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    final Map<String, String> env = new HashMap<String, String>();
    env.put("GIT_BRANCH", config.getGitTagOrBranch());
    env.put("HARVARD_DATA_TOOLS_BASE", config.getEc2GitDir());
    env.put("GENERATOR", config.getCodeGeneratorScript());
    env.put("CONFIG_PATHS", config.getPaths());
    env.put("HARVARD_DATA_GENERATED_OUTPUT", config.getEc2CodeDir());
    env.put("PHASE_0_THREADS", config.getPhase0Threads());
    env.put("PHASE_0_HEAP_SIZE", config.getPhase0HeapSize());
    env.put("PHASE_0_CLASS", config.getPhase0Class());
    env.put("RUN_ID", runId);
    env.put("SERVER_TIMEZONE", config.getServerTimezone());
    env.put("CREATE_PIPELINE", createPipeline ? "1" : "0");
    env.put("CODE_MANAGER_CLASS", config.getCodeManagerClass());
    if (aws.isFile(config.getMavenRepoCacheS3Location())) {
      env.put("MAVEN_REPO_CACHE", AwsUtils.uri(config.getMavenRepoCacheS3Location()));
    }
    env.putAll(getCustomEc2Environment());

    String envString = "";
    for (final String key : env.keySet()) {
      envString += "export " + key + "=\"" + env.get(key) + "\"\n";
    }

    return envString;
  }

  private String getRunId() {
    final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HHmm");
    return config.getDatasetName() + "_" + format.format(new Date());
  }

}
