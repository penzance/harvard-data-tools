package edu.harvard.data.pipeline;

import java.io.IOException;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.CreatePipelineRequest;
import com.amazonaws.services.datapipeline.model.CreatePipelineResult;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionRequest;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionResult;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
  private String pipelineName;
  private String pipelineId;

  public DataPipelineGenerator(final String name, final GenerationSpec spec,
      final DataConfig config) {
    this.name = name;
    this.spec = spec;
    this.config = config;
  }

  public void generate() throws DataConfigurationException, IOException {
    PipelineExecutionRecord.init(config.pipelineDynamoTable);
    final DataPipelineClient client = new DataPipelineClient();
    pipelineName = name + "_Pipeline";
    pipeline = new DataPipeline(config, spec, pipelineName);
    final CreatePipelineRequest create = pipeline.getCreateRequest();
    final CreatePipelineResult createResult = client.createPipeline(create);
    pipelineId = createResult.getPipelineId();
    log.info(createResult);
    populatePipeline();
    final PutPipelineDefinitionRequest definition = pipeline.getDefineRequest(pipelineId);
    final PutPipelineDefinitionResult defineResult = client.putPipelineDefinition(definition);
    logPipelineToDynamo();
    log.info(defineResult);
  }

  private void logPipelineToDynamo() {
    final PipelineExecutionRecord record = new PipelineExecutionRecord(pipelineId);
    record.setPipelineName(pipelineName);
    record.setConfigString(config.paths);
    record.setPipelineCreated(new Date());
    record.save();
  }

  private void populatePipeline() throws DataConfigurationException, JsonProcessingException {
    infrastructure = populateInfrastructure();
    BarrierActivity previousBarrier;
    previousBarrier = populateStartup();
    previousBarrier = populateUnload(previousBarrier);
    previousBarrier = populateS3ToHdfs(previousBarrier);
    previousBarrier = populateHdfsToS3(previousBarrier);

    // Must be last
    populateCleanup(previousBarrier, pipelineId);
  }

  private DataPipelineInfrastructure populateInfrastructure() throws DataConfigurationException {
    final DataPipelineInfrastructure infra = new DataPipelineInfrastructure(pipeline, config, name);
    pipeline.setField("schedule", infra.schedule);
    pipeline.addChild(infra.redshift);
    pipeline.addChild(infra.emr);
    return infra;
  }

  private BarrierActivity populateStartup() throws JsonProcessingException {
    final BarrierActivity barrier = new BarrierActivity(config, "StartupBarrier", infrastructure);
    final String subject = "Activating pipeline " + pipelineName;
    final String msg = "Pipeline started";
    final SnsNotificationPipelineObject startup = new SnsNotificationPipelineObject(config,
        "StartupSNSMessage", subject, msg, config.successSnsArn);
    barrier.setSuccess(startup);
    pipeline.addChild(barrier);
    return barrier;
  }

  private BarrierActivity populateUnload(final BarrierActivity previousBarrier) {
    final BarrierActivity barrier = new BarrierActivity(config, "RedshiftUnloadBarrier",
        infrastructure);

    final String sql = "SELECT * FROM identity_map";
    final S3ObjectId dest = AwsUtils.key(config.workingBucket, pipelineId, "unloaded_tables",
        "identity_map");
    final UnloadTablePipelineActivity unloadId = new UnloadTablePipelineActivity(config,
        infrastructure, "UnloadIdentity", sql, dest, pipelineId);
    unloadId.setDependency(previousBarrier);
    barrier.addDependency(unloadId);

    pipeline.addChild(barrier);
    return barrier;
  }

  private BarrierActivity populateS3ToHdfs(final BarrierActivity previousBarrier) {
    final BarrierActivity barrier = new BarrierActivity(config, "MoveS3ToHdfsBarrier",
        infrastructure);

    final S3ObjectId src = AwsUtils.key(config.workingBucket, pipelineId, "unloaded_tables",
        "identity_map");
    final String dest = "/phase_0/identity_map";
    final MoveS3ToHdfsPipelineActivity moveIdToHdfs = new MoveS3ToHdfsPipelineActivity(config,
        infrastructure, "MoveIdentityToHdfs", src, dest, pipelineId);
    moveIdToHdfs.setDependency(previousBarrier);
    barrier.addDependency(moveIdToHdfs);

    pipeline.addChild(barrier);
    return barrier;
  }

  private BarrierActivity populateHdfsToS3(final BarrierActivity previousBarrier) {
    final BarrierActivity barrier = new BarrierActivity(config, "MoveHdfsToS3Barrier",
        infrastructure);

    final String src = "/phase_0/identity_map";
    final S3ObjectId dest = AwsUtils.key(config.workingBucket, pipelineId, "restored_tables",
        "identity_map");
    final MoveHdfsToS3PipelineActivity moveIdToS3 = new MoveHdfsToS3PipelineActivity(config,
        "MoveIdentityToS3", infrastructure, src, dest, pipelineId);
    moveIdToS3.setDependency(previousBarrier);
    barrier.addDependency(moveIdToS3);

    pipeline.addChild(barrier);
    return barrier;
  }

  private void populateCleanup(final BarrierActivity previousBarrier, final String pipelineId)
      throws JsonProcessingException {
    final PipelineCompletionMessage success = new PipelineCompletionMessage(pipelineId,
        config.reportBucket, config.successSnsArn);
    final String msg = new ObjectMapper().writeValueAsString(success);
    final SnsNotificationPipelineObject completion = new SnsNotificationPipelineObject(config,
        "CompletionSnsAlert", "PipelineSuccess", msg, config.completionSnsArn);
    previousBarrier.setSuccess(completion);
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