package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.datapipeline.model.PipelineDescription;
import com.amazonaws.services.datapipeline.model.PipelineObject;

public class PostMortemReport {
  private String emrUrl;
  private final List<String> steps;
  private final Map<String, PostMortemPipelineObject> pipelineObjects;
  private String failure;
  private PipelineObject pipelineDefinition;
  private PipelineDescription pipelineDescription;

  public PostMortemReport() {
    this.pipelineObjects = new HashMap<String, PostMortemPipelineObject>();
    this.steps = new ArrayList<String>();
  }

  public String getEmrUrl() {
    return emrUrl;
  }

  public void setEmrUrl(final String emrUrl) {
    this.emrUrl = emrUrl;
  }

  public void addPipelineObject(final PostMortemPipelineObject obj) {
    this.pipelineObjects.put(obj.getId(), obj);
  }

  public void setFailure(final String id) {
    this.failure = id;
  }

  public Map<String, PostMortemPipelineObject> getPipelineObjects() {
    return pipelineObjects;
  }

  public String getFailure() {
    return failure;
  }

  public void setPipelineDefinition(final PipelineObject definition) {
    this.pipelineDefinition = definition;
  }

  public PipelineObject getPipelineDefinition() {
    return pipelineDefinition;
  }

  public PipelineDescription getPipelineDescription() {
    return pipelineDescription;
  }

  public void setPipelineDescription(final PipelineDescription description) {
    this.pipelineDescription = description;
  }

  public void addStep(final String id) {
    steps.add(id);
  }

  public List<String> getSteps() {
    return steps;
  }
}
