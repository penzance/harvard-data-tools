package edu.harvard.data.pipeline;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;

public class Phase3PipelineSetup {

  private final PipelineFactory factory;
  private final Pipeline pipeline;
  private final DataConfig config;
  private final S3ObjectId redshiftStagingS3;
  private final S3ObjectId workingDir;

  public Phase3PipelineSetup(final Pipeline pipeline, final PipelineFactory factory) {
    this.factory = factory;
    this.pipeline = pipeline;
    this.config = pipeline.getConfig();
    this.workingDir = AwsUtils.key(config.getS3WorkingLocation(), pipeline.getId());
    this.redshiftStagingS3 = AwsUtils.key(workingDir, config.redshiftStagingDir);
  }

  public PipelineObjectBase populate(final PipelineObjectBase previousPhase) {
    PipelineObjectBase previousStep = previousPhase;
    previousStep = copyDataToS3(previousStep);
    // Update Redshift schema
    previousStep = loadData(previousStep);
    // Delete data from working bucket
    // Move data from incoming bucket to archive
    return previousStep;
  }

  private PipelineObjectBase copyDataToS3(final PipelineObjectBase previousStep) {
    final PipelineObjectBase copy = factory.getS3DistCpActivity("CopyAllTablesToS3", config.getVerifyHdfsDir(2),
        redshiftStagingS3, pipeline.getEmr());
    copy.addDependency(previousStep);
    return copy;
  }

  private PipelineObjectBase loadData(final PipelineObjectBase previousStep) {
    final S3ObjectId script = AwsUtils.key(workingDir, "code", config.redshiftLoadScript);
    final PipelineObjectBase load = factory.getSqlScriptActivity("LoadAllTablesToRedshift", script,
        pipeline.getRedshift(), pipeline.getEmr());
    load.addDependency(previousStep);
    return load;
  }

}
