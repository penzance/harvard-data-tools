package edu.harvard.data.pipeline;

import edu.harvard.data.DataConfig;
import edu.harvard.data.generator.GenerationSpec;

public class Phase2PipelineSetup {

  private final PipelineFactory factory;
  private final Pipeline pipeline;
  private final DataConfig config;
  private final GenerationSpec spec;

  public Phase2PipelineSetup(final Pipeline pipeline, final PipelineFactory factory,
      final GenerationSpec spec) {
    this.factory = factory;
    this.pipeline = pipeline;
    this.spec = spec;
    this.config = pipeline.getConfig();
  }

  public PipelineObjectBase populate(final PipelineObjectBase previousPhase) {
    PipelineObjectBase previousStep = previousPhase;
    previousStep = skipPhase2(previousStep);
    return previousStep;
  }

  private PipelineObjectBase skipPhase2(final PipelineObjectBase previousStep) {
    final String cmd = "hadoop fs -mkdir " + config.getHdfsDir(2) + "; hadoop fs -mv hdfs://"
        + config.getHdfsDir(1) + "/* hdfs://" + config.getHdfsDir(2) + "/";
    final PipelineObjectBase skip = factory.getShellActivity("SkipPhase2", cmd, pipeline.getEmr());
    skip.addDependency(previousStep);
    return skip;
  }

}
