package edu.harvard.data.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.DescribeObjectsRequest;
import com.amazonaws.services.datapipeline.model.DescribeObjectsResult;
import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.Operator;
import com.amazonaws.services.datapipeline.model.OperatorType;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.datapipeline.model.Query;
import com.amazonaws.services.datapipeline.model.QueryObjectsRequest;
import com.amazonaws.services.datapipeline.model.QueryObjectsResult;
import com.amazonaws.services.datapipeline.model.Selector;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;

public class PipelineFailure {

  private final DataConfig config;
  private final String stepId;
  private final String pipelineId;
  private final String emrId;
  private final String activityType;
  private final DataPipelineClient client;

  public static void main(final String[] args) throws IOException, DataConfigurationException {
    final String configPaths = args[0];
    final String stepId = args[1];
    final String pipelineId = args[2];
    final String emrId = args[3];
    final String activityType = args[4];
    final DataConfig config = DataConfig.parseS3Files(DataConfig.class, configPaths, true);
    new PipelineFailure(config, stepId, pipelineId, emrId, activityType).process();
  }

  public PipelineFailure(final DataConfig config, final String stepId, final String pipelineId,
      final String emrId, final String activityType) {
    this.config = config;
    this.stepId = stepId;
    this.pipelineId = pipelineId;
    this.emrId = emrId;
    this.activityType = activityType;
    this.client = new DataPipelineClient();
  }

  public void process() {
    final DescribeObjectsResult stepQuery = query(client, stepId, pipelineId);

    final List<S3ObjectId> paths;
    switch (activityType) {
    case "SqlActivity":
      paths = getSqlActivityPaths();
      break;
    default:
      paths = new ArrayList<S3ObjectId>();
      break;
    }

    sendSnsMessage(paths);
  }

  private void sendSnsMessage(final List<S3ObjectId> paths) {
    final AmazonSNSClient sns = new AmazonSNSClient();
    final String topicArn = config.successSnsArn;
    final String subject = "Step " + stepId + " Failed.";
    String msg = "Logs";
    for (final S3ObjectId path : paths) {
      msg += "\n\n" + consoleUrl(path);
    }
    final PublishRequest publishRequest = new PublishRequest(topicArn, msg, subject);
    sns.publish(publishRequest);
  }

  private List<S3ObjectId> getSqlActivityPaths() {
    final List<S3ObjectId> paths = new ArrayList<S3ObjectId>();
    final DescribeObjectsResult emrQuery = query(client, emrId, pipelineId);
    final PipelineObject emrObject = emrQuery.getPipelineObjects().get(0);
    final String attempt = getFieldRef(emrObject, "@headAttempt");
    paths.add(AwsUtils.key(config.logBucket, pipelineId, emrId, emrObject.getId(), attempt));
    return paths;
  }

  private static String consoleUrl(final S3ObjectId path) {
    return "https://console.aws.amazon.com/s3/home?bucket=" + path.getBucket() + "&prefix="
        + path.getKey();
  }

  private static String getFieldRef(final PipelineObject pipelineObj, final String key) {
    for (final Field field : pipelineObj.getFields()) {
      if (field.getKey().equals(key)) {
        return field.getRefValue();
      }
    }
    return null;
  }

  private static String getField(final PipelineObject pipelineObj, final String key) {
    for (final Field field : pipelineObj.getFields()) {
      if (field.getKey().equals(key)) {
        return field.getStringValue();
      }
    }
    return null;
  }

  private static DescribeObjectsResult query(final DataPipelineClient client, final String objectId,
      final String pipelineId) {
    final Operator operator = new Operator().withValues(objectId).withType(OperatorType.REF_EQ);
    final Selector idSelector = new Selector().withFieldName("parent").withOperator(operator);
    final Query query = new Query().withSelectors(idSelector);
    final QueryObjectsResult objects = client.queryObjects(new QueryObjectsRequest()
        .withQuery(query).withPipelineId(pipelineId).withSphere("INSTANCE"));
    return client.describeObjects(
        new DescribeObjectsRequest().withObjectIds(objects.getIds()).withPipelineId(pipelineId));
  }

}
