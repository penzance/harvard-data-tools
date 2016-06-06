package edu.harvard.data.pipeline;

public class CleanupActivity extends PipelineStep {

  protected CleanupActivity(final DataConfig params, final String id,
      final DataPipelineInfrastructure infra, final String pipelineId) {
    super(params, id, "ShellCommandActivity", infra.emr, infra.pipeline, pipelineId);
  }

}
