package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.services.datapipeline.model.CreatePipelineRequest;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.generator.GenerationSpec;

public class DataPipeline extends AbstractPipelineObject {

  private final String pipelineName;
  private final GenerationSpec spec;
  private String pipelineId;

  protected DataPipeline(final DataConfig config, final GenerationSpec spec, final String name)
      throws DataConfigurationException, JsonProcessingException {
    super(config, "Default", "Default");
    this.spec = spec;
    this.pipelineName = name;
    set("failureAndRerunMode", "CASCADE");
    set("scheduleType", "cron");
    set("role", config.dataPipelineRole);
    set("resourceRole", config.dataPipelineResourceRoleArn);
    set("pipelineLogUri", "s3://" + config.logBucket);
    set("onFail", getFailureObject());
  }

  private AbstractPipelineObject getFailureObject() throws JsonProcessingException {
    final PipelineCompletionMessage completion = new PipelineCompletionMessage(pipelineId,
        config.reportBucket, config.failureSnsArn);
    final String msg = new ObjectMapper().writeValueAsString(completion);
    final SnsNotificationPipelineObject failure = new SnsNotificationPipelineObject(config,
        "FailureSnsAlert", "PipelineFailed", msg, config.completionSnsArn);
    return failure;
  }

  public CreatePipelineRequest getCreateRequest() {
    final CreatePipelineRequest createRequest = new CreatePipelineRequest();
    createRequest.setName(pipelineName);
    createRequest.setUniqueId(UUID.randomUUID().toString());
    return createRequest;
  }

  public PutPipelineDefinitionRequest getDefineRequest(final String pipelineId)
      throws DataConfigurationException {
    final PutPipelineDefinitionRequest defineRequest = new PutPipelineDefinitionRequest();
    defineRequest.setPipelineId(pipelineId);
    final List<PipelineObject> pipelineObjects = new ArrayList<PipelineObject>();
    pipelineObjects.add(getPipelineObject());
    for (final AbstractPipelineObject child : getChildren()) {
      pipelineObjects.add(child.getPipelineObject());
    }
    defineRequest.setPipelineObjects(pipelineObjects);
    return defineRequest;
  }

  public void setField(final String key, final SchedulePipelineObject ref) {
    set(key, ref);
  }

  public void addChild(final AbstractPipelineObject child) {
    children.add(child);
  }
}
