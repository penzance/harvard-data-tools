package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.datapipeline.model.PipelineObject;

public class PostMortemPipelineObject {

  private String id;
  private String name;
  private final PipelineObject pipelineObj;
  private String status;
  private String errorMessage;
  private final List<PipelineLog> logs;

  public PostMortemPipelineObject(final PipelineObject pipelineObj) {
    this.pipelineObj = pipelineObj;
    this.logs = new ArrayList<PipelineLog>();
  }

  public void setName(final String name) {
    this.name = name;
  }

  public void setId(final String id) {
    this.id = id;
  }

  public void addLog(final PipelineLog log) {
    this.logs.add(log);
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public List<PipelineLog> getLogs() {
    return logs;
  }

  public PipelineObject getPipelineObject() {
    return pipelineObj;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(final String status) {
    this.status = status;
  }

  public void setErrorMessage(final String errorMessage) {
    this.errorMessage = errorMessage;
  }

}
