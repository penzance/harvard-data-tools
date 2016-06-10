package edu.harvard.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

import edu.harvard.data.canvas.CanvasDataConfig;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.canvas.data_api.DataDump;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasPhase0Bootstrap {

  private static final Logger log = LogManager.getLogger();

  public static void main(final String[] args)
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    String configPathString = args[0];
    CanvasDataConfig config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class,
        configPathString, false);
    DumpInfo.init(config.getDumpInfoDynamoTable());
    final ApiClient api = new ApiClient(config.getCanvasDataHost(), config.getCanvasApiKey(),
        config.getCanvasApiSecret());
    DataDump dump = null;
    for (final DataDump candidate : api.getDumps()) {
      if (needToSaveDump(candidate)) {
        dump = candidate;
        break;
      }
    }
    if (dump != null) {
      log.info("Saving dump " + dump);
      for (final String path : getInfrastructurePaths(dump)) {
        configPathString += "|" + path;
      }
      config = CanvasDataConfig.parseInputFiles(CanvasDataConfig.class, configPathString, true);
      createPhase0(config);
    }
    // No dump to download
  }

  private static void createPhase0(final CanvasDataConfig config) throws IOException {
    final AmazonEC2Client ec2client = new AmazonEC2Client();

    final LaunchSpecification spec = new LaunchSpecification();
    spec.setImageId(config.getPhase0Ami());
    spec.setInstanceType(config.getPhase0InstanceType());
    spec.setKeyName(config.getKeypair());
    spec.setSubnetId(config.getSubnetId());
    spec.setUserData(getUserData());
    final IamInstanceProfileSpecification instanceProfile = new IamInstanceProfileSpecification();
    instanceProfile.setArn(config.getDataPipelineResourceRoleArn());
    spec.setIamInstanceProfile(instanceProfile);

    final RequestSpotInstancesRequest request = new RequestSpotInstancesRequest();
    request.setSpotPrice(config.getPhase0BidPrice());
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
    // TODO: Check in case the startup failed.
  }

  private static String getUserData() throws IOException {
    final AwsUtils aws = new AwsUtils();
    final S3ObjectId bootstrapScript = AwsUtils
        .key("s3://hdt-code/api_pipeline/phase-0-bootstrap.sh");

    String userData = "";
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

  private static List<String> getInfrastructurePaths(final DataDump dump) {
    final List<String> paths = new ArrayList<String>();
    paths.add("s3://hdt-code/api_pipeline/tiny_phase_0.properties");
    paths.add("s3://hdt-code/api_pipeline/tiny_emr.properties");
    return paths;
  }

  private static boolean needToSaveDump(final DataDump dump) throws IOException {
    final DumpInfo info = DumpInfo.find(dump.getDumpId());
    if (dump.getSequence() < 189) {
      log.warn("Dump downloader set to ignore dumps with sequence < 189");
      return false;
    }
    if (info == null) {
      log.info("Dump needs to be saved; no dump info record for " + dump.getDumpId());
      return true;
    }
    if (info.getDownloaded() == null || !info.getDownloaded()) {
      log.info("Dump needs to be saved; previous download did not complete.");
      return true;
    }
    final Date downloadStart = info.getDownloadStart();
    // Re-download any dump that was updated less than an hour before it was
    // downloaded before.
    final Date conservativeStart = new Date(downloadStart.getTime() - (60 * 60 * 1000));
    if (conservativeStart.before(dump.getUpdatedAt())) {
      log.info(
          "Dump needs to be saved; previously downloaded less than an hour after it was last updated.");
      return true;
    }
    log.info("Dump does not need to be saved; already exists at " + info.getBucket() + "/"
        + info.getKey() + ".");
    return false;
  }

}
