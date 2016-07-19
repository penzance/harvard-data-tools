package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.generator.GeneratedCodeManager;
import edu.harvard.data.identity.IdentityMapHadoopJob;
import edu.harvard.data.identity.IdentityScrubHadoopJob;

public class Phase1PipelineSetup {

  private final S3ObjectId unloadIdentityS3;
  private final String identityHdfs;
  private final S3ObjectId redshiftStagingS3;
  private final S3ObjectId workingDir;
  private final Pipeline pipeline;
  private final PipelineFactory factory;
  private final DataConfig config;
  private final GeneratedCodeManager codeManager;
  private final InputTableIndex dataIndex;
  private final String runId;

  public Phase1PipelineSetup(final Pipeline pipeline, final PipelineFactory factory,
      final GeneratedCodeManager codeManager, final String runId, final InputTableIndex dataIndex) {
    this.factory = factory;
    this.pipeline = pipeline;
    this.codeManager = codeManager;
    this.dataIndex = dataIndex;
    this.runId = runId;
    this.config = pipeline.getConfig();
    this.workingDir = AwsUtils.key(config.getS3WorkingLocation(runId));
    this.unloadIdentityS3 = AwsUtils.key(workingDir, "unloaded_tables", "identity_map");
    this.redshiftStagingS3 = AwsUtils.key(workingDir, config.getRedshiftStagingDir(),
        "identity_map");
    this.identityHdfs = config.getHdfsDir(0) + "/identity_map";
  }

  public PipelineObjectBase populate(final PipelineObjectBase previousPhase) {
    PipelineObjectBase previousStep = previousPhase;
    if (identityPhaseRequired()) {
      previousStep = acquireLease(previousStep, "IdentityLeaseAcquire");

      previousStep = unloadIdentity(previousStep);
      previousStep = refreshLease(previousStep, "AfterUnloadLeaseRefresh");

      previousStep = copyIdentityToHdfs(previousStep);
      previousStep = refreshLease(previousStep, "AfterCopyToHDFSLeaseRefresh");

      previousStep = identityPreverify(previousStep);
      previousStep = hadoopIdentityMap(previousStep);
      previousStep = hadoopIdentityScrub(previousStep);
      previousStep = identityPostverify(previousStep);

      previousStep = copyIdentityToS3(previousStep);
      previousStep = refreshLease(previousStep, "AfterCopyToS3LeaseRefresh");

      previousStep = loadIdentity(previousStep);
      previousStep = releaseLease(previousStep, "IdentityLeaseRelease");
    }
    previousStep = moveUnmodifiedTables(previousStep);
    return previousStep;
  }

  private PipelineObjectBase releaseLease(final PipelineObjectBase previousStep, final String id) {
    final PipelineObjectBase release = factory.getReleaseLeaseActivity(id,
        config.getLeaseDynamoTable(), config.getIdentityLease(), runId, pipeline.getEmr());
    release.addDependency(previousStep);
    return release;
  }

  private PipelineObjectBase acquireLease(final PipelineObjectBase previousStep, final String id) {
    final PipelineObjectBase acquire = factory.getAcquireLeaseActivity(id,
        config.getLeaseDynamoTable(), config.getIdentityLease(), runId,
        config.getIdentityLeaseLengthSeconds(), pipeline.getEmr());
    acquire.addDependency(previousStep);
    return acquire;
  }

  private PipelineObjectBase refreshLease(final PipelineObjectBase previousStep, final String id) {
    final PipelineObjectBase renew = factory.getRenewLeaseActivity(id, config.getLeaseDynamoTable(),
        config.getIdentityLease(), runId, config.getIdentityLeaseLengthSeconds(),
        pipeline.getEmr());
    renew.addDependency(previousStep);
    return renew;
  }

  private boolean identityPhaseRequired() {
    for (final String idTable : codeManager.getIdentityMapperClasses().keySet()) {
      if (dataIndex.containsTable(idTable)) {
        return true;
      }
    }
    return false;
  }

  private PipelineObjectBase moveUnmodifiedTables(final PipelineObjectBase previousStep) {
    final String script = config.getEmrCodeDir() + "/" + config.getMoveUnmodifiedScript(1);
    final PipelineObjectBase move = factory.getShellActivity("Phase1MoveUnmodifiedFiles", script,
        pipeline.getEmr());
    move.addDependency(previousStep);
    return move;
  }

  private PipelineObjectBase identityPreverify(final PipelineObjectBase previousStep) {
    final Class<?> cls = codeManager.getIdentityPreverifyJob();
    if (cls != null) {
      final List<String> args = new ArrayList<String>();
      args.add(config.getPaths());
      args.add(runId);
      final PipelineObjectBase verify = factory.getEmrActivity("IdentityPreverify",
          pipeline.getEmr(), cls, args);
      verify.addDependency(previousStep);
      return verify;
    }
    return previousStep;
  }

  private PipelineObjectBase identityPostverify(final PipelineObjectBase previousStep) {
    final Class<?> cls = codeManager.getIdentityPostverifyJob();
    if (cls != null) {
      final List<String> args = new ArrayList<String>();
      args.add(config.getPaths());
      args.add(runId);
      final PipelineObjectBase verify = factory.getEmrActivity("IdentityPostverify",
          pipeline.getEmr(), cls, args);
      verify.addDependency(previousStep);
      return verify;
    }
    return previousStep;
  }

  private PipelineObjectBase hadoopIdentityMap(final PipelineObjectBase previousStep) {
    final Class<?> cls = IdentityMapHadoopJob.class;
    final List<String> args = new ArrayList<String>();
    args.add(config.getPaths());
    args.add(runId);
    args.add(codeManager.getClass().getCanonicalName());
    final PipelineObjectBase identity = factory.getEmrActivity("IdentityMapHadoop",
        pipeline.getEmr(), cls, args);
    identity.addDependency(previousStep);
    return identity;
  }

  private PipelineObjectBase hadoopIdentityScrub(final PipelineObjectBase previousStep) {
    final Class<?> cls = IdentityScrubHadoopJob.class;
    final List<String> args = new ArrayList<String>();
    args.add(config.getPaths());
    args.add(runId);
    args.add(codeManager.getClass().getCanonicalName());
    final PipelineObjectBase identity = factory.getEmrActivity("IdentityScrubHadoop",
        pipeline.getEmr(), cls, args);
    identity.addDependency(previousStep);
    return identity;
  }

  private PipelineObjectBase unloadIdentity(final PipelineObjectBase previousStep) {
    final String sql = "SELECT * FROM pii.identity_map";
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
    final S3ObjectId script = AwsUtils.key(workingDir, "code",
        config.getIdentityRedshiftLoadScript());
    final PipelineObjectBase load = factory.getSqlScriptActivity("LoadIdentityToRedshift", script,
        pipeline.getRedshift(), pipeline.getEmr());
    load.addDependency(previousStep);
    return load;
  }
}
