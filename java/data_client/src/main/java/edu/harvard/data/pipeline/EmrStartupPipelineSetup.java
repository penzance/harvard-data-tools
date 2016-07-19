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
  private final String runId;

  public EmrStartupPipelineSetup(final Pipeline pipeline, final PipelineFactory factory,
      final String runId) {
    this.factory = factory;
    this.config = pipeline.getConfig();
    this.pipeline = pipeline;
    this.runId = runId;
    this.workingDir = AwsUtils.key(config.getS3WorkingLocation(runId));
  }

  public PipelineObjectBase populate() {
    final PipelineObjectBase barrier = factory.getSynchronizationBarrier("SetupCompleteBarrier",
        pipeline.getEmr());
    final PipelineObjectBase startupLogging = startupLogging();
    final PipelineObjectBase copyGeneratedCode = copyGeneratedCode();
    final PipelineObjectBase copyData = copyData();
    barrier.addDependency(startupLogging);
    barrier.addDependency(copyGeneratedCode);
    barrier.addDependency(copyData);
    return barrier;
  }

  private PipelineObjectBase startupLogging() {
    final Class<?> cls = PipelineStartup.class;
    final List<String> args = new ArrayList<String>();
    args.add(runId); // args[0] in main class
    args.add(config.getPipelineDynamoTable()); // args[1] in main class
    final String jar = config.getEmrCodeDir() + "/" + config.getDataToolsJar();
    return factory.getJavaShellActivity("PipelineStartup", jar, cls, args, pipeline.getEmr());
  }

  private PipelineObjectBase copyGeneratedCode() {
    final String src = config.getEmrCodeDir();
    final S3ObjectId dest = AwsUtils.key(workingDir, "code");
    return factory.getS3CopyActivity("CopyGeneratedCode", src, dest, pipeline.getEmr());
  }

  private PipelineObjectBase copyData() {
    final String manifest = config.getEmrCodeDir() + "/" + config.getS3ToHdfsManifestFile();
    return factory.getS3DistCpActivity("CopyDataToHdfs", manifest, workingDir, config.getHdfsDir(0),
        pipeline.getEmr());
  }

}
