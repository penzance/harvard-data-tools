package edu.harvard.data.pipeline;

public class S3CopyActivity extends PipelineStep {

  protected S3CopyActivity(final DataConfig params, final String id,
      final DataPipelineInfrastructure infra, final String src, final String dest) {
    super(params, id, "ShellCommandActivity", infra.emr, infra.pipeline, infra.pipelineId);
    set("command", "aws s3 cp " + src + " " + dest);
  }

}
