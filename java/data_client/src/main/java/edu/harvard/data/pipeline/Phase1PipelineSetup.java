package edu.harvard.data.pipeline;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;

public class Phase1PipelineSetup {

  private final S3ObjectId unloadIdentityS3;
  private final String identityHdfs;
  private final S3ObjectId redshiftStagingS3;
  private final S3ObjectId workingDir;
  private final Pipeline pipeline;
  private final PipelineFactory factory;
  private final DataConfig config;

  public Phase1PipelineSetup(final Pipeline pipeline, final PipelineFactory factory) {
    this.factory = factory;
    this.pipeline = pipeline;
    this.config = pipeline.getConfig();
    this.workingDir = AwsUtils.key(config.workingBucket, pipeline.getId());
    this.unloadIdentityS3 = AwsUtils.key(workingDir, "unloaded_tables", "identity_map");
    this.redshiftStagingS3 = AwsUtils.key(workingDir, config.redshiftStagingDir, "identity_map");
    this.identityHdfs = "/phase_0/identity_map";
  }

  public PipelineObjectBase populate(final PipelineObjectBase previousStep) {
    // Take out ID lease
    final PipelineObjectBase unload = unloadIdentity(previousStep);
    final PipelineObjectBase s3ToHdfs = copyIdentityToHdfs(unload);
    // Hadoop identity jobs
    // Hadoop scrub jobs
    final PipelineObjectBase hdfsToS3 = copyIdentityToS3(s3ToHdfs);
    final PipelineObjectBase load = loadIdentity(hdfsToS3);
    // Release ID lease
    return load;
  }

  private PipelineObjectBase unloadIdentity(final PipelineObjectBase previousStep) {
    final String sql = "SELECT * FROM identity_map";
    final PipelineObjectBase unloadId = factory.getUnloadActivity("UnloadIdentity", sql,
        unloadIdentityS3, pipeline.getRedshift(), pipeline.getEmr());
    unloadId.addDependency(previousStep);
    return unloadId;
  }

  private PipelineObjectBase copyIdentityToHdfs(final PipelineObjectBase previousStep) {
    final PipelineObjectBase copy = factory.getS3DistCpActivity("CopyIdentityToHdfs",
        unloadIdentityS3, identityHdfs, pipeline.getEmr());
    copy.addDependency(previousStep);
    return copy;
  }

  private PipelineObjectBase copyIdentityToS3(final PipelineObjectBase previousStep) {
    final PipelineObjectBase copy = factory.getS3DistCpActivity("CopyIdentityToS3", identityHdfs,
        redshiftStagingS3, pipeline.getEmr());
    copy.addDependency(previousStep);
    return copy;
  }

  private PipelineObjectBase loadIdentity(final PipelineObjectBase previousStep) {
    final S3ObjectId script = AwsUtils.key(workingDir, "code", config.identityRedshiftLoadScript);
    final PipelineObjectBase load = factory.getSqlScriptActivity("LoadIdentityToRedshift", script,
        pipeline.getRedshift(), pipeline.getEmr());
    load.addDependency(previousStep);
    return load;
  }
}
