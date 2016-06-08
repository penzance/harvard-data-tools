package edu.harvard.data.pipeline;

import edu.harvard.data.DataConfig;

public class Phase3PipelineSetup {

  private final PipelineFactory factory;
  private final Pipeline pipeline;
  private final DataConfig config;

  public Phase3PipelineSetup(final Pipeline pipeline, final PipelineFactory factory) {
    this.factory = factory;
    this.pipeline = pipeline;
    this.config = pipeline.getConfig();
  }

  public PipelineObjectBase populate(final PipelineObjectBase previousStep) {
    return previousStep;
  }

}
