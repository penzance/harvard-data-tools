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
  private S3ObjectId outputKey;
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

  public PipelineComplete() throws IOException {
    this.pipelineClient = new DataPipelineClient();
    this.s3Client = new AmazonS3Client();
    this.snsClient = new AmazonSNSClient();
    this.report = new PostMortemReport();
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
      this.pipelineId = data.get("pipelineId");
      this.runId = data.get("runId");
      this.outputKey = key(data.get("reportBucket"), this.pipelineId + ".json");
      this.snsArn = data.get("snsArn");
      PipelineExecutionRecord.init(data.get("pipelineDynamoTable"));
      this.process();
    } catch (final IOException e) {
      return e.toString();
    }
    return "";
  }

  public void process() throws IOException {
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
    updateDynamo();
    sendSnsMessage();
  }

  private void updateDynamo() {
    final PipelineExecutionRecord record = PipelineExecutionRecord.find(runId);
    // TODO: Check if record doesn't exist.
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
      final PostMortemPipelineObject obj = new PostMortemPipelineObject(pipelineObj);
      obj.setName(getRefField(pipelineObj.getFields(), "parent"));
      obj.setId(pipelineObj.getId());
      obj.setStatus(getStringField(pipelineObj.getFields(), "@status"));

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

  private String reportUrl() {
    return "http://localhost:8000/data_dashboard/pipeline/" + pipelineId;
  }

  public S3ObjectId key(final String bucket, final String... keys) {
    String key = "";
    for (final String k : keys) {
      key += "/" + k;
    }
    return new S3ObjectId(bucket, key.substring(1));
  }

  private void sendSnsMessage() {
    String subject = report.getPipelineDescription().getName() + " ";
    final String failure = report.getFailure();
    String msg = "Report at " + reportUrl();
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
