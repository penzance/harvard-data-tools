package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;

public class EmrStartupPipelineSetup {

  private final DataConfig config;
  private final S3ObjectId workingDir;
  private final Pipeline pipeline;
  private final PipelineFactory factory;
  private final String pipelineId;

  public EmrStartupPipelineSetup(final Pipeline pipeline, final PipelineFactory factory) {
    this.factory = factory;
    this.config = pipeline.getConfig();
    this.pipeline = pipeline;
    this.pipelineId = pipeline.getId();
    this.workingDir = AwsUtils.key(config.workingBucket, pipelineId);
  }

  public PipelineObjectBase populate() {
    final PipelineObjectBase barrier = factory.getSynchronizationBarrier("SetupCompleteBarrier",
        pipeline.getEmr());
    final PipelineObjectBase startupLogging = startupLogging();
    final PipelineObjectBase copyGeneratedCode = copyGeneratedCode();
    barrier.addDependency(startupLogging);
    barrier.addDependency(copyGeneratedCode);
    return barrier;
  }

  private PipelineObjectBase startupLogging() {
    final String jar = config.emrCodeDir + "/" + config.dataToolsJar;
    final String cls = PipelineStartup.class.getCanonicalName();
    final List<String> args = new ArrayList<String>();
    args.add(pipelineId); // args[0] in main class
    args.add(config.pipelineDynamoTable); // args[1] in main class
    return factory.getEmrActivity("PipelineStartup", pipeline.getEmr(), jar, cls, args);
  }

  private PipelineObjectBase copyGeneratedCode() {
    final String src = config.emrCodeDir;
    final S3ObjectId dest = AwsUtils.key(workingDir, "code");
    return factory.getS3CopyActivity("CopyGeneratedCode", src, dest, pipeline.getEmr());
  }
}
