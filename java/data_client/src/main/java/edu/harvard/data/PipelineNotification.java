package edu.harvard.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.S3ObjectSummary;
//import com.amazonaws.services.sns.AmazonSNSClient; // Deprecated
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

public class PipelineNotification {
  public static void main(final String[] args) throws IOException {
    final AwsUtils aws = new AwsUtils();
    String message = "";
    for (final S3ObjectId log : getInterestingLogs()) {
      final List<String> errors = getErrors(aws, log);
      if (!errors.isEmpty()) {
        message += getLink(log)+ "\n";
        message += log.getKey().substring(log.getKey().lastIndexOf("/") + 1) + "\n\n";
        for (final String error : errors) {
          message += error + "\n";
        }
        message += "\n";
      }
    }

    // XXX Strip out access tokens

    final AmazonSNS client = AmazonSNSClientBuilder.defaultClient();
    final String subject = "Message published by Java.";
    final String topicArn = "arn:aws:sns:us-east-1:364469542718:hdtdevcanvas-SuccessSNS-8HQ4921XICVD";
    final PublishRequest publishRequest = new PublishRequest(topicArn, message, subject);
    final PublishResult publishResult = client.publish(publishRequest);
    System.out.println(publishResult);
  }

  public static List<S3ObjectId> getInterestingLogs() throws IOException {
    final List<S3ObjectId> logs = new ArrayList<S3ObjectId>();
    final String logBucket = "hdtdevcanvas-loggings3bucket-128rr4494bdx8";
    final String pipelineId = "df-04699922RSS7CT7WJ3F4";
    final String clusterName = "EmrClusterName";
    final String emrKey = pipelineId + "/" + clusterName;

    final AwsUtils aws = new AwsUtils();
    for (final S3ObjectId dir : aws
        .listDirectories(AwsUtils.key(logBucket, pipelineId, clusterName))) {
      final String emrId = dir.getKey().substring(emrKey.length() + 1);
      if (emrId.startsWith("@")) {
        final S3ObjectId logDir = AwsUtils.key(logBucket, pipelineId, clusterName, emrId,
            emrId + "_Attempt=1");
        for (final S3ObjectSummary obj : aws.listKeys(logDir)) {
          if (obj.getKey().substring(logDir.getKey().length()).startsWith("/TaskRunner")) {
            logs.add(AwsUtils.key(obj));
          }
        }
      }
    }
    return logs;
  }

  private static String getLink(final S3ObjectId log) {
    final String dir = log.getKey().substring(0, log.getKey().lastIndexOf("/"));
    final String url = "https://console.aws.amazon.com/s3/home?bucket=" + log.getBucket()
    + "&prefix=" + dir;
    return url;
  }

  private static List<String> getErrors(final AwsUtils aws, final S3ObjectId s3ObjectId)
      throws IOException {
    final List<String> errors = new ArrayList<String>();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(
        aws.getInputStream(s3ObjectId, s3ObjectId.getKey().toLowerCase().endsWith("gz"))))) {
      String line = in.readLine();
      while (line != null) {
        if (line.contains("ERROR")) {
          errors.add(line);
        }
        line = in.readLine();
      }
    }
    return errors;
  }

}
