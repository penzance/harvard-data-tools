package edu.harvard.data.pipeline;

public class FailurePipelineObject extends AbstractPipelineObject {

  protected FailurePipelineObject(final DataConfig params, final String stepId,
      final DataPipeline pipeline, final AbstractPipelineObject infrastructure,
      final String activityType) {
    super(params, stepId + "Failure", "EmrActivity");
    final String command = "/home/hadoop/data_tools.jar," + params.paths + "," + stepId + ","
        + pipeline.id + "," + infrastructure.id + "," + type;
    set("runsOn", infrastructure);
    set("step", command);
  }
}
