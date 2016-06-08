package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionRequest;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.generator.GenerationSpec;

public class Pipeline {

  private final DataConfig config;
  private final String pipelineId;
  private final String name;
  private final PipelineFactory factory;
  private final PipelineObjectBase redshift;
  private final PipelineObjectBase emr;
  private final PipelineObjectBase schedule;

  public Pipeline(final String name, final GenerationSpec spec, final DataConfig config,
      final String pipelineId, final PipelineFactory factory) throws JsonProcessingException {
    this.name = name;
    this.config = config;
    this.pipelineId = pipelineId;
    this.factory = factory;
    this.schedule = factory.getSchedule();
    this.redshift = factory.getRedshift();
    this.emr = createEmr();
    createDefaultObject();
  }

  public PutPipelineDefinitionRequest getDefineRequest(final String pipelineId)
      throws DataConfigurationException {
    final PutPipelineDefinitionRequest defineRequest = new PutPipelineDefinitionRequest();
    defineRequest.setPipelineId(pipelineId);
    defineRequest.setPipelineObjects(factory.getAllObjects());
    return defineRequest;
  }

  private PipelineObjectBase createDefaultObject()
      throws JsonProcessingException {
    final PipelineObjectBase defaultObj = factory.getDefault(schedule);
    final PipelineCompletionMessage completion = new PipelineCompletionMessage(pipelineId,
        config.reportBucket, config.completionSnsArn, config.pipelineDynamoTable);
    final String failMsg = new ObjectMapper().writeValueAsString(completion);
    final String failSubj = "Pipeline " + name + " Failed";
    defaultObj.set("onFail", factory.getSns("FailureSnsAlert", failSubj, failMsg, config.completionSnsArn));
    return defaultObj;
  }

  private PipelineObjectBase createEmr() {
    final PipelineObjectBase emr = factory.getEmr(name + "_Emr_Cluster");
    emr.set("bootstrapAction", emrBootstrapAction());
    return emr;
  }

  private String emrBootstrapAction() {
    final S3ObjectId script = AwsUtils.key(config.codeBucket, config.gitTagOrBranch,
        "bootstrap.sh");
    final List<String> bootstrapParams = new ArrayList<String>();
    bootstrapParams.add(AwsUtils.uri(script));
    bootstrapParams.add(config.dataSourceSchemaVersion);
    bootstrapParams.add(config.gitTagOrBranch);
    bootstrapParams.add(config.datasource.toLowerCase() + "_generate_tools.py");
    bootstrapParams.add(config.paths);
    bootstrapParams.add(pipelineId);
    bootstrapParams.add(config.emrCodeDir);
    return StringUtils.join(bootstrapParams, ",");
  }

  public DataConfig getConfig() {
    return config;
  }

  public String getId() {
    return pipelineId;
  }

  public PipelineObjectBase getEmr() {
    return emr;
  }

  public PipelineObjectBase getRedshift() {
    return redshift;
  }

}
