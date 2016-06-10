package edu.harvard.data.pipeline;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.generator.GeneratedCodeManager;

public class Phase3PipelineSetup {

  private final PipelineFactory factory;
  private final Pipeline pipeline;
  private final DataConfig config;
  private final S3ObjectId redshiftStagingS3;
  private final S3ObjectId workingDir;
  private final GeneratedCodeManager codeManager;

  public Phase3PipelineSetup(final Pipeline pipeline, final PipelineFactory factory,
      final GeneratedCodeManager codeManager) {
    this.factory = factory;
    this.pipeline = pipeline;
    this.codeManager = codeManager;
    this.config = pipeline.getConfig();
    this.workingDir = AwsUtils.key(config.getS3WorkingLocation(), pipeline.getId());
    this.redshiftStagingS3 = AwsUtils.key(workingDir, config.getRedshiftStagingDir());
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
    final PipelineObjectBase copy = factory.getS3DistCpActivity("CopyAllTablesToS3",
        config.getHdfsDir(getLastPhase()), redshiftStagingS3, pipeline.getEmr());
    copy.addDependency(previousStep);
    return copy;
  }

  private PipelineObjectBase loadData(final PipelineObjectBase previousStep) {
    final S3ObjectId script = AwsUtils.key(workingDir, "code", config.getRedshiftLoadScript());
    final PipelineObjectBase load = factory.getSqlScriptActivity("LoadAllTablesToRedshift", script,
        pipeline.getRedshift(), pipeline.getEmr());
    load.addDependency(previousStep);
    return load;
  }

  private int getLastPhase() {
    int last = 0;
    for (final Integer i : codeManager.getHadoopProcessingJobs().keySet()) {
      if (i > last) {
        last = i;
      }
    }
    return last;
  }

}
