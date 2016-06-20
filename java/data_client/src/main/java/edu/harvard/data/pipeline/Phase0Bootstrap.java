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
import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.util.Base64;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public abstract class Phase0Bootstrap {

  private static final Logger log = LogManager.getLogger();

  protected DataConfig config;
  private String configPathString;
  private Class<? extends DataConfig> configClass;
  protected String runId;
  private boolean createPipeline;
  protected AwsUtils aws;

  protected abstract List<S3ObjectId> getInfrastructureConfigPaths();

  protected abstract Map<String, String> getCustomEc2Environment();

  protected abstract boolean newDataAvailable()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException;

  protected void init(final String configPathString, final Class<? extends DataConfig> configClass,
      final boolean createPipeline) throws IOException, DataConfigurationException {
    this.configPathString = configPathString;
    this.configClass = configClass;
    this.createPipeline = createPipeline;
    this.config = DataConfig.parseInputFiles(configClass, configPathString, false);
    this.runId = getRunId();
    this.aws = new AwsUtils();
  }

  protected void run()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    if (newDataAvailable()) {
      for (final S3ObjectId path : getInfrastructureConfigPaths()) {
        configPathString += "|" + AwsUtils.uri(path);
      }
      this.config = DataConfig.parseInputFiles(configClass, configPathString, false);
      createPhase0();
    }
  }

  private void createPhase0() throws IOException {
    final AmazonEC2Client ec2client = new AmazonEC2Client();

    final LaunchSpecification spec = new LaunchSpecification();
    spec.setImageId(config.getPhase0Ami());
    spec.setInstanceType(config.getPhase0InstanceType());
    spec.setKeyName(config.getKeypair());
    spec.setSubnetId(config.getSubnetId());
    spec.setUserData(getUserData(config));
    final IamInstanceProfileSpecification instanceProfile = new IamInstanceProfileSpecification();
    instanceProfile.setArn(config.getDataPipelineCreatorRoleArn());
    spec.setIamInstanceProfile(instanceProfile);

    final RequestSpotInstancesRequest request = new RequestSpotInstancesRequest();
    request.setSpotPrice(config.getPhase0BidPrice());
    // request.setAvailabilityZoneGroup(config.getPhase0AvailabilityZoneGroup());
    request.setInstanceCount(1);
    request.setLaunchSpecification(spec);

    final RequestSpotInstancesResult result = ec2client.requestSpotInstances(request);
    System.out.println(result);

    final List<String> instanceIds = new ArrayList<String>();
    instanceIds.add(result.getSpotInstanceRequests().get(0).getSpotInstanceRequestId());
    final DescribeSpotInstanceRequestsRequest describe = new DescribeSpotInstanceRequestsRequest();
    describe.setSpotInstanceRequestIds(instanceIds);
    final DescribeSpotInstanceRequestsResult description = ec2client
        .describeSpotInstanceRequests(describe);
    System.out.println(description);
    // TODO: final Check in case final the startup failed.
  }

  private String getUserData(final DataConfig config) throws IOException {
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

  private String getBootstrapEnvironment(final DataConfig config) {
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
    env.put("PIPELINE_SETUP_CLASS", config.getPipelineSetupClass());
    env.put("SERVER_TIMEZONE", config.getServerTimezone());
    env.put("CREATE_PIPELINE", createPipeline ? "1" : "0");
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
