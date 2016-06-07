package edu.harvard.data.pipeline;

public class BarrierActivity extends AbstractPipelineObject {

  protected BarrierActivity(final DataConfig params, final String id,
      final DataPipelineInfrastructure infra) {
    super(params, id, "ShellCommandActivity");
    set("runsOn", infra.emr);
    set("command", "ls -l");
  }

  void addDependency(final PipelineStep step) {
    set("dependsOn", step);
  }

}
