package edu.harvard.data.pipeline;

import java.util.Date;
import java.util.List;

//import com.amazonaws.services.datapipeline.DataPipelineClient; // deprecated
import com.amazonaws.services.datapipeline.DataPipelineClientBuilder;
import com.amazonaws.services.datapipeline.DataPipeline;
import com.amazonaws.services.datapipeline.model.DescribeObjectsRequest;
import com.amazonaws.services.datapipeline.model.DescribeObjectsResult;
import com.amazonaws.services.datapipeline.model.Field;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.datapipeline.model.Query;
import com.amazonaws.services.datapipeline.model.QueryObjectsRequest;
import com.amazonaws.services.datapipeline.model.QueryObjectsResult;
//import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient; // deprecated
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.Instance;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesRequest;
import com.amazonaws.services.elasticmapreduce.model.ListInstancesResult;

// Run by the StartupActivity step
public class PipelineStartup {

  public static void main(final String[] args) {
    final String runId = args[0];
    final String dynamoTable = args[1];

    PipelineExecutionRecord.init(dynamoTable);

    final PipelineExecutionRecord record = PipelineExecutionRecord.find(runId);
    final String emrId = getEmrId(record.getPipelineId());
    final String emrMasterIp = getEmrMasterIp(emrId);

    record.setPipelineStart(new Date());
    record.setStatus(PipelineExecutionRecord.Status.PipelineRunning.toString());
    record.setEmrId(emrId);
    record.setEmrMasterIp(emrMasterIp);
    record.save();
  }

  private static String getEmrId(final String pipelineId) {
    final DataPipeline client = DataPipelineClientBuilder.defaultClient();
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

  private static String getEmrMasterIp(final String emrId) {
    final AmazonElasticMapReduce client = AmazonElasticMapReduceClientBuilder.defaultClient();
    final DescribeClusterResult cluster = client.describeCluster(new DescribeClusterRequest().withClusterId(emrId));
    final ListInstancesResult instances = client.listInstances(new ListInstancesRequest().withClusterId(emrId));

    final String masterDns = cluster.getCluster().getMasterPublicDnsName();
    for (final Instance instance : instances.getInstances()) {
      if (instance.getPrivateDnsName().equals(masterDns)) {
        return instance.getPrivateIpAddress();
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
