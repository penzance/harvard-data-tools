package edu.harvard.data.pipeline;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;

public class RedshiftLoadPipelineActivity extends PipelineStep {

  public RedshiftLoadPipelineActivity(final DataConfig config, final String id,
      final DataPipelineInfrastructure infra, final S3ObjectId src, final String pipelineId,
      final S3ObjectId script, final S3ObjectId stagingDir) {
    super(config, id, "SqlActivity", infra.emr, infra.pipeline, pipelineId);
    set("scriptUri", AwsUtils.uri(script));
    set("database", infra.redshift);
  }

}
