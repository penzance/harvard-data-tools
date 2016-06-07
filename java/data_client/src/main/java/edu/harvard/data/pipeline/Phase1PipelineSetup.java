package edu.harvard.data.pipeline;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.generator.GenerationSpec;

public class Phase1PipelineSetup {

  private final GenerationSpec spec;
  private final DataConfig config;
  private final DataPipelineInfrastructure infrastructure;
  private final String pipelineId;
  private final S3ObjectId unloadIdentityS3;
  private final String identityHdfs;
  private final S3ObjectId redshiftStagingS3;
  private final S3ObjectId workingDir;

  public Phase1PipelineSetup(final GenerationSpec spec, final DataConfig config,
      final DataPipelineInfrastructure infrastructure) {
    this.spec = spec;
    this.config = config;
    this.infrastructure = infrastructure;
    this.pipelineId = infrastructure.pipelineId;
    this.workingDir = AwsUtils.key(config.workingBucket, pipelineId);
    this.unloadIdentityS3 = AwsUtils.key(workingDir, "unloaded_tables", "identity_map");
    this.redshiftStagingS3 = AwsUtils.key(workingDir, "redshift_staging", "identity_map");
    this.identityHdfs = "/phase_0/identity_map";
  }

  public AbstractPipelineObject populate(final AbstractPipelineObject previousStep) {
    // Take out ID lease

    // Unload identity map
    final PipelineStep unload = unloadIdentity(previousStep);

    // Copy identity map to HDFS
    final PipelineStep s3ToHdfs = copyIdentityToHdfs(unload);

    // Identity hadoop jobs
    final PipelineStep identityHadoop = s3ToHdfs;
    // Scrub hadoop jobs
    final PipelineStep scrubHadoop = identityHadoop;

    // Copy identity map to S3
    final PipelineStep hdfsToS3 = copyIdentityToS3(scrubHadoop);

    // Load identity map to Redshift
    final PipelineStep load = loadIdentity(hdfsToS3);

    // Release ID lease
    final PipelineStep releaseLease = load;

    return releaseLease;
  }

  private PipelineStep unloadIdentity(final AbstractPipelineObject previousStep) {
    final String sql = "SELECT * FROM identity_map";
    final UnloadTablePipelineActivity unloadId = new UnloadTablePipelineActivity(config,
        infrastructure, "UnloadIdentity", sql, unloadIdentityS3, pipelineId);
    unloadId.setDependency(previousStep);
    return unloadId;
  }

  private PipelineStep copyIdentityToHdfs(final AbstractPipelineObject previousStep) {
    final MoveS3ToHdfsPipelineActivity moveIdToHdfs = new MoveS3ToHdfsPipelineActivity(config,
        infrastructure, "CopyIdentityToHdfs", unloadIdentityS3, identityHdfs, pipelineId);
    moveIdToHdfs.setDependency(previousStep);
    return moveIdToHdfs;
  }

  private PipelineStep copyIdentityToS3(final AbstractPipelineObject previousStep) {
    final MoveHdfsToS3PipelineActivity moveIdToS3 = new MoveHdfsToS3PipelineActivity(config,
        "CopyIdentityToS3", infrastructure, identityHdfs, redshiftStagingS3, pipelineId);
    moveIdToS3.setDependency(previousStep);
    return moveIdToS3;
  }

  private PipelineStep loadIdentity(final AbstractPipelineObject previousStep) {
    final S3ObjectId script = AwsUtils.key(workingDir, "code", config.identityRedshiftLoadScript);
    final RedshiftLoadPipelineActivity load = new RedshiftLoadPipelineActivity(config,
        "LoadIdentityToRedshift", infrastructure, redshiftStagingS3, pipelineId, script, redshiftStagingS3);
    load.setDependency(previousStep);
    return load;
  }
}
