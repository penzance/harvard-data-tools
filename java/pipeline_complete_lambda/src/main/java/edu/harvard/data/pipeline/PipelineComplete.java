package edu.harvard.data.pipeline;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.DescribeObjectsRequest;
import com.amazonaws.services.datapipeline.model.DescribeObjectsResult;
import com.amazonaws.services.datapipeline.model.DescribePipelinesRequest;
import com.amazonaws.services.datapipeline.model.DescribePipelinesResult;
import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.GetPipelineDefinitionRequest;
import com.amazonaws.services.datapipeline.model.GetPipelineDefinitionResult;
import com.amazonaws.services.datapipeline.model.PipelineDescription;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.datapipeline.model.Query;
import com.amazonaws.services.datapipeline.model.QueryObjectsRequest;
import com.amazonaws.services.datapipeline.model.QueryObjectsResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.harvard.data.pipeline.PipelineExecutionRecord.Status;

public class PipelineComplete implements RequestHandler<SNSEvent, String> {
  private static final Logger log = LogManager.getLogger();

  private final DataPipelineClient pipelineClient;
  private final AmazonS3Client s3Client;
  private final AmazonSNSClient snsClient;
  private String pipelineId;
  private String runId;
  private String emrInstanceName;
  private String emrName;
  private String emrAttempt;
  private String region;
  private String logBucket;
  private String emrResourceId;
  private final PostMortemReport report;
  private DescribeObjectsResult objectDescriptions;
  private String snsArn;
  private String logGroupName;
  private String logStreamName;
  private PipelineExecutionRecord record;

  public PipelineComplete() throws IOException {
    this.pipelineClient = new DataPipelineClient();
    this.s3Client = new AmazonS3Client();
    this.snsClient = new AmazonSNSClient();
    this.report = new PostMortemReport();
  }

  public static void main(final String[] args) throws IOException {
    final String pipelineId = "df-0839553BTYV3JX6PCZT";
    final String runId = "dce_matterhorn_2016-07-22-1247";
    final S3ObjectId outputKey = key("hdt-pipeline-reports", runId + ".json");
    final String snsArn = "arn:aws:sns:us-east-1:364469542718:hdtdevcanvas-SuccessSNS-8HQ4921XICVD";
    final String pipelineDynamoTable = "hdt-pipeline-dev";
    final String logGroupName = "";
    final String logStreamName = "";
    new PipelineComplete().process(pipelineId, runId, outputKey, snsArn, pipelineDynamoTable,
        logGroupName, logStreamName);
  }

  @Override
  @SuppressWarnings("unchecked")
  public String handleRequest(final SNSEvent input, final Context context) {
    log.info("Handling request");
    final String json = input.getRecords().get(0).getSNS().getMessage();
    log.info("JSON: " + json);
    log.info("Logs: " + context.getLogStreamName());
    final ObjectMapper mapper = new ObjectMapper();
    Map<String, String> data;
    try {
      data = mapper.readValue(json, Map.class);
      final String pipelineId = data.get("pipelineId");
      final String runId = data.get("runId");
      final S3ObjectId outputKey = key(data.get("reportBucket"), this.runId + ".json");
      final String snsArn = data.get("snsArn");
      final String pipelineDynamoTable = data.get("pipelineDynamoTable");
      final String logGroupName = context.getLogGroupName();
      final String logStreamName = context.getLogStreamName();

      this.process(pipelineId, runId, outputKey, snsArn, pipelineDynamoTable, logGroupName,
          logStreamName);
    } catch (final IOException e) {
      return e.toString();
    }
    return "";
  }

  public void process(final String pipelineId, final String runId, final S3ObjectId outputKey,
      final String snsArn, final String pipelineDynamoTable, final String logGroupName,
      final String logStreamName) throws IOException {
    this.pipelineId = pipelineId;
    this.runId = runId;
    this.snsArn = snsArn;
    this.logGroupName = logGroupName;
    this.logStreamName = logStreamName;

    PipelineExecutionRecord.init(pipelineDynamoTable);
    this.record = PipelineExecutionRecord.find(runId);
    saveInitialDynamoData();

    getPipelineDefinition();
    getPipelineObjects();
    populateLogs();
    getStepOrder();
    report.setEmrUrl(emrUrl());
    final ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    final String json = mapper.writeValueAsString(report);
    final byte[] bytes = json.getBytes();
    final ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(bytes.length);
    s3Client.putObject(outputKey.getBucket(), outputKey.getKey(), new ByteArrayInputStream(bytes),
        metadata);
    System.out.println("Wrote output to " + outputKey);
    saveFinalDynamoData();
    sendSnsMessage();
  }

  private void saveInitialDynamoData() {
    record.setCleanupLogGroup(logGroupName);
    record.setCleanupLogStream(logStreamName);
    record.save();
  }

  private void saveFinalDynamoData() {
    record.setPipelineEnd(new Date());
    if (report.getFailure() == null) {
      record.setStatus(Status.Success.toString());
    } else {
      record.setStatus(Status.Failed.toString());
    }
    record.save();
  }

  private void getPipelineObjects() {
    final Query query = new Query();
    final QueryObjectsResult objects = pipelineClient.queryObjects(new QueryObjectsRequest()
        .withQuery(query).withPipelineId(pipelineId).withSphere("INSTANCE"));
    objectDescriptions = pipelineClient.describeObjects(
        new DescribeObjectsRequest().withObjectIds(objects.getIds()).withPipelineId(pipelineId));
    for (final PipelineObject pipelineObj : objectDescriptions.getPipelineObjects()) {
      // Get the parent object in order to find any static properties
      final String parentId = getRefField(pipelineObj.getFields(), "parent");
      final PipelineObject parentObj = pipelineClient
          .describeObjects(new DescribeObjectsRequest()
              .withObjectIds(Collections.singleton(parentId)).withPipelineId(pipelineId))
          .getPipelineObjects().get(0);

      final PostMortemPipelineObject obj = new PostMortemPipelineObject(pipelineObj);
      obj.setName(parentObj.getName());
      obj.setId(pipelineObj.getId());
      obj.setStatus(getStringField(pipelineObj.getFields(), "@status"));
      final String stdout = getStringField(parentObj.getFields(), "stdout");
      if (stdout != null) {
        obj.addLog(s3Url(key(stdout.substring(0, stdout.lastIndexOf("/"))), "stdout"));
      }

      if (obj.getStatus().equals("FAILED")) {
        report.setFailure(obj.getId());
      }
      if (getStringField(pipelineObj.getFields(), "type").equals("EmrCluster")) {
        emrInstanceName = obj.getId();
        emrName = obj.getName();
        emrAttempt = getRefField(pipelineObj.getFields(), "@headAttempt");
        emrResourceId = getStringField(pipelineObj.getFields(), "@resourceId");
        region = getStringField(pipelineObj.getFields(), "@resourceRegion");
      }
      report.addPipelineObject(obj);
    }
  }

  private void getStepOrder() {
    final Map<String, PostMortemPipelineObject> pipelineObjects = report.getPipelineObjects();
    final Map<String, PostMortemPipelineObject> objects = new HashMap<String, PostMortemPipelineObject>();
    for (final String objName : pipelineObjects.keySet()) {
      final PipelineObject obj = pipelineObjects.get(objName).getPipelineObject();
      final String endTime = getStringField(obj.getFields(), "@actualEndTime");
      if (endTime != null) {
        objects.put(endTime, pipelineObjects.get(objName));
      }
    }
    final List<String> times = new ArrayList<String>(objects.keySet());
    Collections.sort(times);
    for (final String time : times) {
      final PostMortemPipelineObject obj = objects.get(time);
      report.addStep(obj.getId());
    }
  }

  private void populateLogs() {
    for (final PipelineObject pipelineObj : objectDescriptions.getPipelineObjects()) {
      final PostMortemPipelineObject obj = report.getPipelineObjects().get(pipelineObj.getId());
      for (final PipelineLog log : getLogUrls(pipelineObj, obj.getName())) {
        obj.addLog(log);
      }
    }
  }

  private void getPipelineDefinition() {
    final DescribePipelinesResult describeResult = pipelineClient
        .describePipelines(new DescribePipelinesRequest().withPipelineIds(pipelineId));
    final GetPipelineDefinitionResult defineResult = pipelineClient
        .getPipelineDefinition(new GetPipelineDefinitionRequest().withPipelineId(pipelineId));

    final PipelineDescription description = describeResult.getPipelineDescriptionList().get(0);

    PipelineObject definition = null;
    // Find the default issue in the list
    for (final PipelineObject pipelineObject : defineResult.getPipelineObjects()) {
      if (pipelineObject.getId().equals("Default")) {
        definition = pipelineObject;
      }
    }
    report.setPipelineDefinition(definition);
    report.setPipelineDescription(description);
    logBucket = getStringField(definition.getFields(), "pipelineLogUri")
        .substring("s3://".length());
  }

  private List<PipelineLog> getLogUrls(final PipelineObject obj, final String stepName) {
    final String type = getStringField(obj.getFields(), "type");
    final List<PipelineLog> logs = new ArrayList<PipelineLog>();
    switch (type) {
    case "SqlActivity":
      getTaskExecutorPaths(obj, logs);
      break;
    case "ShellCommandActivity":
      getTaskExecutorPaths(obj, logs);
      getStepPath(obj, stepName, logs);
      getContainerUrl(logs);
      break;
    case "EmrActivity":
      getTaskExecutorPaths(obj, logs);
      break;
    case "EmrCluster":
      break;
    default:
      System.out.println("Unknown type: " + type);
      break;
    }
    return logs;
  }

  private String getStringField(final List<Field> fields, final String key) {
    final List<Field> lst = getFields(fields, key);
    if (!lst.isEmpty()) {
      return lst.get(0).getStringValue();
    }
    return null;
  }

  private String getRefField(final List<Field> fields, final String key) {
    final List<Field> lst = getFields(fields, key);
    if (!lst.isEmpty()) {
      return lst.get(0).getRefValue();
    }
    return null;
  }

  private List<Field> getFields(final List<Field> fields, final String key) {
    final List<Field> lst = new ArrayList<Field>();
    for (final Field fld : fields) {
      if (fld.getKey().equals(key)) {
        lst.add(fld);
      }
    }
    return lst;
  }

  private void getStepPath(final PipelineObject step, final String stepName,
      final List<PipelineLog> logs) {
    if (!getStringField(step.getFields(), "@status").equals("CASCADE_FAILED")) {
      final S3ObjectId path = key(logBucket, pipelineId, stepName, step.getId(),
          getRefField(step.getFields(), "@headAttempt"));
      logs.add(s3Url(path, "Step"));
    }
  }

  private void getContainerUrl(final List<PipelineLog> logs) {
    final S3ObjectId path = key(logBucket, pipelineId, emrName, emrInstanceName, emrAttempt,
        emrResourceId, "containers");
    logs.add(s3Url(path, "Container"));
  }

  private void getTaskExecutorPaths(final PipelineObject obj, final List<PipelineLog> logs) {
    final S3ObjectId path = key(logBucket, pipelineId, emrName, emrInstanceName, emrAttempt);
    logs.add(s3Url(path, "Task Executor"));
  }

  private static PipelineLog s3Url(final S3ObjectId path, final String label) {
    final String url = "https://console.aws.amazon.com/s3/home?bucket=" + path.getBucket()
    + "&prefix=" + path.getKey();
    return new PipelineLog(label, url);
  }

  private String emrUrl() {
    return "https://console.aws.amazon.com/elasticmapreduce/home?region=" + region
        + "#cluster-details:" + emrResourceId;
  }

  // Copied from AwsUtils.java; referencing that file would require depending on
  // data_tools, which would push the JAR file size over Lambda's limits.
  public static S3ObjectId key(final String bucket, final String... keys) {
    String key = "";
    for (final String k : keys) {
      key += "/" + k;
    }
    return new S3ObjectId(bucket, key.substring(1));
  }

  public static S3ObjectId key(String str) {
    if (str.toLowerCase().startsWith("s3://")) {
      str = str.substring("s3://".length());
    }
    return key(str.substring(0, str.indexOf("/")), str.substring(str.indexOf("/") + 1));
  }

  private void sendSnsMessage() {
    String subject = report.getPipelineDescription().getName() + " ";
    final String failure = report.getFailure();
    String msg = "Pipeline Complete";
    if (failure == null) {
      subject += "Succeeded";
    } else {
      subject += "Failed at Step " + failure;
      msg += "\n\nInteresting logs:";
      for (final PipelineLog log : report.getPipelineObjects().get(failure).getLogs()) {
        msg += "\n\n" + log;
      }
    }
    final PublishRequest publishRequest = new PublishRequest(snsArn, msg, subject);
    snsClient.publish(publishRequest);
  }

}
