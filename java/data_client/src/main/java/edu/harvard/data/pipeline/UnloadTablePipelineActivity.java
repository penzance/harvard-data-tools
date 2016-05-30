package edu.harvard.data.pipeline;

import com.amazonaws.services.s3.model.S3ObjectId;

public class UnloadTablePipelineActivity extends PipelineStep {

  protected UnloadTablePipelineActivity(final DataConfig conf,
      final DataPipelineInfrastructure infra, final String id, final String sql,
      final S3ObjectId dest) {
    super(conf, id, "SqlActivity", infra.emr, infra.pipeline);
    set("script", generateQuery(conf, sql, dest));
    set("database", infra.redshift);
  }

  private String generateQuery(final DataConfig conf, final String sql, final S3ObjectId dest) {
    return "UNLOAD ('" + sql + "') TO 's3://" + dest.getBucket() + dest.getKey()
    + "' WITH CREDENTIALS AS 'aws_access_key_id=" + conf.awsKeyId + ";aws_secret_access_key="
    + conf.awsSecretKey + "' delimiter '\\t' GZIP;";
  }

}
