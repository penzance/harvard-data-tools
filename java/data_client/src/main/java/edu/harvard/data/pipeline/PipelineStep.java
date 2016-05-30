package edu.harvard.data.pipeline;

public abstract class PipelineStep extends AbstractPipelineObject {

  AbstractPipelineObject dependsOn;
  AbstractPipelineObject infrastructure;
  private final DataPipeline pipeline;

  protected PipelineStep(final DataConfig params, final String id, final String type,
      final AbstractPipelineObject infrastructure, final DataPipeline pipeline) {
    super(params, id, type);
    this.infrastructure = infrastructure;
    this.pipeline = pipeline;

    set("retryDelay", "2 Minutes");
    set("runsOn", infrastructure);

    final AbstractPipelineObject success = getSuccessObject();
    set("onSuccess", success);
    final AbstractPipelineObject failure = getFailureObject();
    set("onFail", failure);
  }

  private AbstractPipelineObject getFailureObject() {
    return new FailurePipelineObject(params, id, pipeline, infrastructure, type);
  }

  private AbstractPipelineObject getSuccessObject() {
    final SnsNotificationPipelineObject success = new SnsNotificationPipelineObject(params,
        id + "Success");
    success.setMessage("There are lots of things that could go in here...");
    success.setSubject("Activity " + id + " Success.");
    success.setTopicArn(params.successSnsArn);
    return success;
  }

  protected String cleanHdfs(final String path) {
    if (path.toLowerCase().startsWith("hdfs://")) {
      return path.substring("hdfs://".length());
    }
    return path;
  }

  protected String cleanS3(final String path) {
    if (path.toLowerCase().startsWith("s3://")) {
      return path.substring("s3://".length());
    }
    return path;
  }

  void setDependency(final AbstractPipelineObject dependsOn) {
    set("dependsOn", new ActionSetup(params, id, infrastructure, dependsOn));
  }

}
