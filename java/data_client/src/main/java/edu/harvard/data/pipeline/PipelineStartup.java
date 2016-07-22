package edu.harvard.data.pipeline;

import java.util.Date;
import java.util.List;

import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.DescribeObjectsRequest;
import com.amazonaws.services.datapipeline.model.DescribeObjectsResult;
import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.datapipeline.model.Query;
import com.amazonaws.services.datapipeline.model.QueryObjectsRequest;
import com.amazonaws.services.datapipeline.model.QueryObjectsResult;

// Run by the StartupActivity step
public class PipelineStartup {

  public static void main(final String[] args) {
    final String runId = args[0];
    final String dynamoTable = args[1];

    PipelineExecutionRecord.init(dynamoTable);

    final PipelineExecutionRecord record = PipelineExecutionRecord.find(runId);
    final String emrId = getEmrId(record.getPipelineId());

    record.setPipelineStart(new Date());
    record.setStatus(PipelineExecutionRecord.Status.PipelineRunning.toString());
    record.setEmrId(emrId);
    record.save();
  }

  private static String getEmrId(final String pipelineId) {
    final DataPipelineClient client = new DataPipelineClient();
    final QueryObjectsResult objects = client.queryObjects(new QueryObjectsRequest()
        .withQuery(new Query()).withPipelineId(pipelineId).withSphere("INSTANCE"));
    final DescribeObjectsResult descriptions = client.describeObjects(
        new DescribeObjectsRequest().withObjectIds(objects.getIds()).withPipelineId(pipelineId));
    for (final PipelineObject obj : descriptions.getPipelineObjects()) {
      if (getField(obj.getFields(), "type").equals("EmrCluster")) {
        return getField(obj.getFields(), "@resourceId");
      }
    }
    return null;
  }

  private static String getField(final List<Field> fields, final String key) {
    for (final Field fld : fields) {
      if (fld.getKey().equals(key)) {
        return fld.getStringValue();
      }
    }
    return null;
  }

}
