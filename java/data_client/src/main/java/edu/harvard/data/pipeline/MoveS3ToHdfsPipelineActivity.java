package edu.harvard.data.pipeline;

import com.amazonaws.services.s3.model.S3ObjectId;

public class MoveS3ToHdfsPipelineActivity extends PipelineStep {

  protected MoveS3ToHdfsPipelineActivity(final DataConfig params,
      final DataPipelineInfrastructure infra, final String id, final S3ObjectId src,
      final String dest) {
    super(params, id, "ShellCommandActivity", infra.emr, infra.pipeline);
    set("command", generateCommand(src, dest));
  }

  private String generateCommand(final S3ObjectId src, final String dest) {
    final String srcString = "s3://" + src.getBucket() + src.getKey();
    return "s3-dist-cp --src=s3://" + srcString + " --dest=hdfs://" + cleanHdfs(dest)
    + " --outputCodec=none";
  }

}
