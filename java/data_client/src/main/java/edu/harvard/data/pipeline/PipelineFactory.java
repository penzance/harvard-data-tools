package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;

public class PipelineFactory {
  private final DataConfig config;
  private final List<PipelineObjectBase> allObjects;

  public PipelineFactory(final DataConfig config, final String pipelineId) {
    this.config = config;
    this.allObjects = new ArrayList<PipelineObjectBase>();
  }

  public PipelineObjectBase getSchedule() {
    final PipelineObjectBase schedule = new PipelineObjectBase(config, "DefaultSchedule",
        "Schedule");
    schedule.setName("RunOnce");
    schedule.set("occurrences", "1");
    schedule.set("startAt", "FIRST_ACTIVATION_DATE_TIME");
    schedule.set("period", "1 Day");
    allObjects.add(schedule);
    return schedule;
  }

  public PipelineObjectBase getDefault(final PipelineObjectBase schedule) {
    final PipelineObjectBase defaultObj = new PipelineObjectBase(config, "Default", "Default");
    defaultObj.set("failureAndRerunMode", "CASCADE");
    defaultObj.set("scheduleType", "cron");
    defaultObj.set("role", config.dataPipelineRole);
    defaultObj.set("resourceRole", config.dataPipelineResourceRoleArn);
    defaultObj.set("pipelineLogUri", "s3://" + config.logBucket);
    defaultObj.set("schedule", schedule);
    allObjects.add(defaultObj);
    return defaultObj;
  }

  public PipelineObjectBase getSns(final String id, final String subject, final String msg,
      final String topicArn) {
    final PipelineObjectBase sns = new PipelineObjectBase(config, id, "SnsAlarm");
    sns.set("role", config.dataPipelineRole);
    sns.set("subject", subject);
    sns.set("message", msg);
    sns.set("topicArn", topicArn);
    allObjects.add(sns);
    return sns;
  }

  public PipelineObjectBase getRedshift() {
    final PipelineObjectBase redshift = new PipelineObjectBase(config, "RedshiftDatabase",
        "RedshiftDatabase");
    redshift.set("clusterId", config.redshiftCluster);
    redshift.set("username", config.redshiftUserName);
    redshift.set("*password", config.redshiftPassword);
    redshift.set("databaseName", config.redshiftDatabase);
    allObjects.add(redshift);
    return redshift;
  }

  public PipelineObjectBase getEmr(final String name) {
    final PipelineObjectBase emr = new PipelineObjectBase(config, name, "EmrCluster");
    emr.set("useOnDemandOnLastAttempt", "true");
    emr.set("keyPair", config.keypair);
    emr.set("releaseLabel", config.emrReleaseLabel);
    emr.set("terminateAfter", config.emrTerminateAfter);
    emr.set("subnetId", config.appSubnet);
    emr.set("masterInstanceType", config.emrMasterInstanceType);
    if (config.emrMasterBidPrice != null) {
      emr.set("masterInstanceBidPrice", config.emrMasterBidPrice);
    }
    if (Integer.parseInt(config.emrCoreInstanceCount) > 0) {
      emr.set("coreInstanceType", config.emrCoreInstanceType);
      emr.set("coreInstanceCount", config.emrCoreInstanceCount);
      if (config.emrCoreBidPrice != null) {
        emr.set("coreInstanceBidPrice", config.emrCoreBidPrice);
      }
    }
    if (Integer.parseInt(config.emrTaskInstanceCount) > 0) {
      emr.set("taskInstanceType", config.emrTaskInstanceType);
      emr.set("taskInstanceCount", config.emrTaskInstanceCount);
      if (config.emrTaskBidPrice != null) {
        emr.set("taskInstanceBidPrice", config.emrTaskBidPrice);
      }
    }
    allObjects.add(emr);
    return emr;
  }

  public PipelineObjectBase getSynchronizationBarrier(final String id, final PipelineObjectBase infrastructure) {
    final PipelineObjectBase barrier = new PipelineObjectBase(config, id, "ShellCommandActivity");
    barrier.set("command", "ls");
    barrier.set("runsOn", infrastructure);
    allObjects.add(barrier);
    return barrier;
  }

  public PipelineObjectBase getEmrActivity(final String id, final PipelineObjectBase infrastructure,
      final String jar, final String cls, final List<String> args) {
    final PipelineObjectBase emr = new PipelineObjectBase(config, id, "EmrActivity");
    final String params = StringUtils.join(args, ",");
    emr.set("step", jar + "," + cls + "," + params);
    emr.set("retryDelay", "2 Minutes");
    emr.set("runsOn", infrastructure);
    allObjects.add(emr);
    return emr;
  }

  public PipelineObjectBase getS3CopyActivity(final String id, final S3ObjectId src,
      final String dest, final PipelineObjectBase infrastructure) {
    final PipelineObjectBase copy = new PipelineObjectBase(config, id, "ShellCommandActivity");
    copy.set("command", "aws s3 cp " + AwsUtils.uri(src) + " " + dest + " --recursive");
    copy.set("runsOn", infrastructure);
    allObjects.add(copy);
    return copy;
  }

  public PipelineObjectBase getS3CopyActivity(final String id, final String src,
      final S3ObjectId dest, final PipelineObjectBase infrastructure) {
    final PipelineObjectBase copy = new PipelineObjectBase(config, id, "ShellCommandActivity");
    copy.set("command", "aws s3 cp " + src + " " + AwsUtils.uri(dest) + " --recursive");
    copy.set("runsOn", infrastructure);
    allObjects.add(copy);
    return copy;
  }

  public PipelineObjectBase getUnloadActivity(final String id, final String sql,
      final S3ObjectId dest, final PipelineObjectBase database, final PipelineObjectBase infrastructure) {
    final PipelineObjectBase unload = new PipelineObjectBase(config, id, "SqlActivity");
    final String query = "UNLOAD ('" + sql + "') TO '" + AwsUtils.uri(dest)
    + "/' WITH CREDENTIALS AS " + getCredentials() + " delimiter '\\t' GZIP;";
    unload.set("script", query);
    unload.set("database", database);
    unload.set("runsOn", infrastructure);
    allObjects.add(unload);
    return unload;
  }

  public PipelineObjectBase getSqlScriptActivity(final String id, final S3ObjectId script,
      final PipelineObjectBase database, final PipelineObjectBase infrastructure) {
    final PipelineObjectBase sql = new PipelineObjectBase(config, id, "SqlActivity");
    sql.set("scriptUri", AwsUtils.uri(script));
    sql.set("database", database);
    sql.set("runsOn", infrastructure);
    allObjects.add(sql);
    return sql;
  }

  public PipelineObjectBase getS3DistCpActivity(final String id, final S3ObjectId src,
      final String dest, final PipelineObjectBase infrastructure) {
    final PipelineObjectBase move = new PipelineObjectBase(config, id, "ShellCommandActivity");
    final String cmd = "s3-dist-cp --src=" + AwsUtils.uri(src) + " --dest=hdfs://" + cleanHdfs(dest)
    + " --outputCodec=none";
    move.set("command", cmd);
    move.set("runsOn", infrastructure);
    allObjects.add(move);
    return move;
  }

  public PipelineObjectBase getS3DistCpActivity(final String id, final String src,
      final S3ObjectId dest, final PipelineObjectBase infrastructure) {
    final PipelineObjectBase move = new PipelineObjectBase(config, id, "ShellCommandActivity");
    final String cmd = "s3-dist-cp --src=hdfs://" + cleanHdfs(src) + " --dest=" + AwsUtils.uri(dest)
    + " --outputCodec=gzip";
    move.set("command", cmd);
    move.set("runsOn", infrastructure);
    allObjects.add(move);
    return move;
  }

  ////////////////

  private String cleanHdfs(final String path) {
    if (path.toLowerCase().startsWith("hdfs://")) {
      return path.substring("hdfs://".length());
    }
    return path;
  }

  private String getCredentials() {
    return "'aws_access_key_id=" + config.awsKeyId + ";aws_secret_access_key=" + config.awsSecretKey
        + "'";
  }

  public List<PipelineObject> getAllObjects() {
    final List<PipelineObject> objects = new ArrayList<PipelineObject>();
    for (final PipelineObjectBase obj : allObjects) {
      objects.add(obj.getPipelineObject());
    }
    return objects;
  }

}
