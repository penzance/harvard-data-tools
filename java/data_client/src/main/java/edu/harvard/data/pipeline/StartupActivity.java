package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class StartupActivity extends PipelineStep {

  protected StartupActivity(final DataConfig params, final String id,
      final DataPipelineInfrastructure infra, final String pipelineId) {
    super(params, id, "EmrActivity", infra.emr, infra.pipeline, pipelineId);
    set("step", getLaunchString());
  }

  private String getLaunchString() {
    final List<String> args = new ArrayList<String>();
    args.add(config.dataToolsJar); // Jar name
    args.add(PipelineStartup.class.getCanonicalName()); // Class name
    args.add(pipelineId); // args[0] in main class
    args.add(config.pipelineDynamoTable); // args[1] in main class
    return StringUtils.join(args, ",");
  }

}
