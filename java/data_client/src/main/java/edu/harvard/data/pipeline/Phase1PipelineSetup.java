package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Mapper;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.generator.GeneratedCodeManager;

public class Phase1PipelineSetup {

  private final S3ObjectId unloadIdentityS3;
  private final String identityHdfs;
  private final S3ObjectId redshiftStagingS3;
  private final S3ObjectId workingDir;
  private final Pipeline pipeline;
  private final PipelineFactory factory;
  private final DataConfig config;
  private final GeneratedCodeManager codeManager;

  public Phase1PipelineSetup(final Pipeline pipeline, final PipelineFactory factory,
      final GeneratedCodeManager codeManager) {
    this.factory = factory;
    this.pipeline = pipeline;
    this.codeManager = codeManager;
    this.config = pipeline.getConfig();
    this.workingDir = AwsUtils.key(config.getS3WorkingLocation(), pipeline.getId());
    this.unloadIdentityS3 = AwsUtils.key(workingDir, "unloaded_tables", "identity_map");
    this.redshiftStagingS3 = AwsUtils.key(workingDir, config.redshiftStagingDir, "identity_map");
    this.identityHdfs = config.getHdfsDir(0) + "/identity_map";
  }

  public PipelineObjectBase populate(final PipelineObjectBase previousPhase) {
    PipelineObjectBase previousStep = previousPhase;
    if (!codeManager.getIdentityMapperClasses().isEmpty()) {
      // Take out ID lease
      previousStep = unloadIdentity(previousStep);
      previousStep = copyIdentityToHdfs(previousStep);
      // Hadoop preverify
      previousStep = hadoopIdentityMap(previousStep);
      previousStep = hadoopIdentityScrub(previousStep);
      // Hadoop postverify
      previousStep = copyIdentityToS3(previousStep);
      previousStep = loadIdentity(previousStep);
      // Release ID lease
    }
    return previousStep;
  }

  private PipelineObjectBase hadoopIdentityMap(final PipelineObjectBase previousStep) {
    final Class<?> cls = codeManager.getIdentityMapHadoopJob();
    final List<String> args = new ArrayList<String>();
    final PipelineObjectBase identity = factory.getEmrActivity("IdentityHadoop", pipeline.getEmr(), cls, args);
    identity.addDependency(previousStep);
    return identity;
  }

  @SuppressWarnings("rawtypes")
  private PipelineObjectBase hadoopIdentityScrub(PipelineObjectBase previousStep) {
    for (final Class<? extends Mapper> cls : codeManager.getIdentityScrubberClasses()) {
      final List<String> args = new ArrayList<String>();
      args.add(config.paths);
      final PipelineObjectBase step = factory.getEmrActivity(cls.getSimpleName(), pipeline.getEmr(), cls, args);
      step.addDependency(previousStep);
      previousStep = step;
    }
    return previousStep;
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
