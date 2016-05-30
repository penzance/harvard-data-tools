package edu.harvard.data.pipeline;

class ActionSetup extends AbstractPipelineObject {

  protected ActionSetup(final DataConfig params, final String id,
      final AbstractPipelineObject infrastructure, final AbstractPipelineObject dependsOn) {
    super(params, id + "LoggingSetup", "ShellCommandActivity");
    set("runsOn", infrastructure);
    if (dependsOn != null) {
      set("dependsOn", dependsOn);
    }
    set("command", "ls -l");
  }

}