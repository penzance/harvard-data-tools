package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

class PipelineStepSetupActivity extends AbstractPipelineObject {

  protected PipelineStepSetupActivity(final DataConfig params, final String id,
      final AbstractPipelineObject infrastructure, final AbstractPipelineObject dependsOn,
      final String pipelineId, final String step) {
    super(params, id + "LoggingSetup", "EmrActivity");
    set("runsOn", infrastructure);
    if (dependsOn != null) {
      set("dependsOn", dependsOn);
    }
    set("step", getLaunchString(pipelineId, step));
  }

  private String getLaunchString(final String pipelineId, final String step) {
    final List<String> args = new ArrayList<String>();
    args.add(config.emrCodeDir + "/" + config.dataToolsJar); // Jar name
    args.add(PipelineStepSetup.class.getCanonicalName()); // Class name
    args.add(pipelineId); // args[0] in main class
    args.add(config.pipelineDynamoTable); // args[1] in main class
    args.add(step); // args[2] in main class
    return StringUtils.join(args, ",");
  }

}