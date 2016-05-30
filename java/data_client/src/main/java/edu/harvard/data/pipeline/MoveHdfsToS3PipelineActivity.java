package edu.harvard.data.pipeline;

import com.amazonaws.services.s3.model.S3ObjectId;

public class MoveHdfsToS3PipelineActivity extends PipelineStep {

  protected MoveHdfsToS3PipelineActivity(final DataConfig params, final String id,
      final DataPipelineInfrastructure infra, final String src,
      final S3ObjectId dest) {
    super(params, id, "ShellCommandActivity", infra.emr, infra.pipeline);
    set("command", getCommand(src, dest));
  }

  protected String getCommand(final String src, final S3ObjectId dest) {
    return "s3-dist-cp --src=hdfs://" + cleanHdfs(src) + " --dest=s3://" + dest.getBucket()
    + dest.getKey() + " --outputCodec=none";
  }

}
