package edu.harvard.data.pipeline;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;

public class EmrStartupPipelineSetup {

  private final DataConfig config;
  private final DataPipelineInfrastructure infrastructure;
  private final String pipelineId;
  private final S3ObjectId workingDir;

  public EmrStartupPipelineSetup(final DataConfig config,
      final DataPipelineInfrastructure infrastructure) {
    this.config = config;
    this.infrastructure = infrastructure;
    this.pipelineId = infrastructure.pipelineId;
    this.workingDir = AwsUtils.key(config.workingBucket, pipelineId);
  }

  public AbstractPipelineObject populate() {
    final BarrierActivity barrier = new BarrierActivity(config, "StartupActionsBarrier",
        infrastructure);

    // Run startup logging step
    final PipelineStep startupLogging = startupLogging();

    // Copy generated code to working bucket
    final PipelineStep copyGeneratedCode = copyGeneratedCode();

    barrier.addDependency(startupLogging);
    barrier.addDependency(copyGeneratedCode);

    return barrier;
  }

  private PipelineStep startupLogging() {
    return new StartupActivity(config, "PipelineStartup", infrastructure);
  }

  private PipelineStep copyGeneratedCode() {
    final String src = config.emrCodeDir;
    final String dest = AwsUtils.uri(AwsUtils.key(workingDir, "code"));
    return new S3CopyActivity(config, "CopyGeneratedCode", infrastructure, src, dest);
  }
}
