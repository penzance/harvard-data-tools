package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import edu.harvard.data.DataConfig;

class PipelineStepSetupActivity extends PipelineObjectBase {

  protected PipelineStepSetupActivity(final DataConfig params, final String id,
      final PipelineObjectBase infrastructure, final PipelineObjectBase dependsOn,
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
    args.add(config.getEmrCodeDir() + "/" + config.getDataToolsJar()); // Jar name
    args.add(PipelineStepSetup.class.getCanonicalName()); // Class name
    args.add(pipelineId); // args[0] in main class
    args.add(config.getPipelineDynamoTable()); // args[1] in main class
    args.add(step); // args[2] in main class
    return StringUtils.join(args, ",");
  }

}