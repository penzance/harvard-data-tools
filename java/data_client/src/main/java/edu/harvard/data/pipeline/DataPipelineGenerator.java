package edu.harvard.data.pipeline;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.CreatePipelineRequest;
import com.amazonaws.services.datapipeline.model.CreatePipelineResult;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionRequest;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionResult;
import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.generator.GenerationSpec;

public class DataPipelineGenerator {
  private static final Logger log = LogManager.getLogger();

  private final GenerationSpec spec;
  private final DataConfig config;

  private DataPipeline pipeline;
  private DataPipelineInfrastructure infrastructure;

  private final String name;

  public DataPipelineGenerator(final String name, final GenerationSpec spec,
      final DataConfig config) {
    this.name = name;
    this.spec = spec;
    this.config = config;
  }

  public String generate() throws DataConfigurationException, IOException {
    final DataPipelineClient client = new DataPipelineClient();
    pipeline = new DataPipeline(config, spec, name + "_Pipeline");
    populatePipeline();
    final CreatePipelineRequest create = pipeline.getCreateRequest();
    final CreatePipelineResult createResult = client.createPipeline(create);
    log.info(createResult);
    final PutPipelineDefinitionRequest definition = pipeline
        .getDefineRequest(createResult.getPipelineId());
    final PutPipelineDefinitionResult defineResult = client.putPipelineDefinition(definition);
    log.info(defineResult);
    return createResult.getPipelineId();
  }

  private void populatePipeline() throws DataConfigurationException {
    infrastructure = populateInfrastructure();
    final BarrierActivity unload = populateUnload();
    final BarrierActivity s3ToHdfs = populateS3ToHdfs(unload);
    final BarrierActivity hdfsToS3 = populateHdfsToS3(s3ToHdfs);
  }

  private DataPipelineInfrastructure populateInfrastructure() throws DataConfigurationException {
    final DataPipelineInfrastructure infra = new DataPipelineInfrastructure(pipeline, config, name);
    pipeline.setField("schedule", infra.schedule);
    pipeline.addChild(infra.redshift);
    pipeline.addChild(infra.emr);
    return infra;
  }

  private BarrierActivity populateUnload() {
    final BarrierActivity barrier = new BarrierActivity(config, "RedshiftUnloadBarrier",
        infrastructure);

    final String sql = "SELECT * FROM identity_map";
    final S3ObjectId dest = AwsUtils.key(config.workingBucket, "unloaded_tables", "identity_map");
    final UnloadTablePipelineActivity unloadId = new UnloadTablePipelineActivity(config,
        infrastructure, "UnloadIdentity", sql, dest);
    barrier.addDependency(unloadId);

    pipeline.addChild(barrier);
    return barrier;
  }

  private BarrierActivity populateS3ToHdfs(final BarrierActivity previousBarrier) {
    final BarrierActivity barrier = new BarrierActivity(config, "MoveS3ToHdfsBarrier",
        infrastructure);

    final S3ObjectId src = AwsUtils.key(config.workingBucket, "unloaded_tables", "identity_map");
    final String dest = "/phase_0/identity_map";
    final MoveS3ToHdfsPipelineActivity moveIdToHdfs = new MoveS3ToHdfsPipelineActivity(config,
        infrastructure, "MoveIdentityToHdfs", src, dest);
    moveIdToHdfs.setDependency(previousBarrier);
    barrier.addDependency(moveIdToHdfs);

    pipeline.addChild(barrier);
    return barrier;
  }

  private BarrierActivity populateHdfsToS3(final BarrierActivity previousBarrier) {
    final BarrierActivity barrier = new BarrierActivity(config, "MoveHdfsToS3Barrier",
        infrastructure);

    final String src = "/phase_0/identity_map";
    final S3ObjectId dest = AwsUtils.key(config.workingBucket, "restored_tables", "identity_map");
    final MoveHdfsToS3PipelineActivity moveIdToS3 = new MoveHdfsToS3PipelineActivity(config,
        "MoveIdentityToS3", infrastructure, src, dest);
    moveIdToS3.setDependency(previousBarrier);
    barrier.addDependency(moveIdToS3);

    pipeline.addChild(barrier);
    return barrier;
  }

}

class DataPipelineInfrastructure {
  final SchedulePipelineObject schedule;
  final RedshiftPipelineObject redshift;
  final EmrPipelineObject emr;
  final DataPipeline pipeline;

  public DataPipelineInfrastructure(final DataPipeline pipeline, final DataConfig config,
      final String name) throws DataConfigurationException {
    this.pipeline = pipeline;
    schedule = new SchedulePipelineObject(config, "DefaultSchedule");
    schedule.setName("RunOnce");
    redshift = new RedshiftPipelineObject(config, "RedshiftDatabase");
    emr = new EmrPipelineObject(config, name + "_Emr_Cluster");
  }
}