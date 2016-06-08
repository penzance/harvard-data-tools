package edu.harvard.data.pipeline;

import edu.harvard.data.DataConfig;

public class Phase2PipelineSetup {

  private final PipelineFactory factory;
  private final Pipeline pipeline;
  private final DataConfig config;
  private final int subPhase;

  public Phase2PipelineSetup(final Pipeline pipeline, final PipelineFactory factory, final int subPhase) {
    this.factory = factory;
    this.pipeline = pipeline;
    this.subPhase = subPhase;
    this.config = pipeline.getConfig();
  }

  public PipelineObjectBase populate(final PipelineObjectBase previousStep) {
    return previousStep;
  }


}
