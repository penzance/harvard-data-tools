package edu.harvard.data.pipeline;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.ActivatePipelineRequest;
import com.amazonaws.services.datapipeline.model.CreatePipelineRequest;
import com.amazonaws.services.datapipeline.model.CreatePipelineResult;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionRequest;
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.CodeManager;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class DataPipelineSetup {
  private static final Logger log = LogManager.getLogger();

  private final DataConfig config;
  private final CodeManager codeManager;
  private final String runId;
  private final InputTableIndex dataIndex;
  private String pipelineId;

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, VerificationException, ClassNotFoundException,
  InstantiationException, IllegalAccessException {
    final String configPath = args[0];
    final String runId = args[1];
    final String codeManagerClassName = args[2];

    final CodeManager codeManager = CodeManager.getCodeManager(codeManagerClassName);
    final DataConfig config = codeManager.getDataConfig(configPath, true);
    final AwsUtils aws = new AwsUtils();
    final InputTableIndex dataIndex = aws.readJson(config.getIndexFileS3Location(runId),
        InputTableIndex.class);
    final DataPipelineSetup pipeline = new DataPipelineSetup(config, dataIndex, codeManager, runId);
    pipeline.generate();
  }

  public DataPipelineSetup(final DataConfig config, final InputTableIndex dataIndex,
      final CodeManager codeManager, final String runId) {
    this.config = config;
    this.dataIndex = dataIndex;
    this.codeManager = codeManager;
    this.runId = runId;
  }

  public void generate() throws DataConfigurationException, IOException {
    log.info("Data at " + dataIndex);
    PipelineExecutionRecord.init(config.getPipelineDynamoTable());
    final DataPipelineClient client = new DataPipelineClient();
    final CreatePipelineRequest create = getCreateRequest();
    final CreatePipelineResult createResult = client.createPipeline(create);
    pipelineId = createResult.getPipelineId();
    log.info(createResult);

    final Pipeline pipeline = populatePipeline();

    final PutPipelineDefinitionRequest definition = pipeline.getDefineRequest(pipelineId);
    final PutPipelineDefinitionResult defineResult = client.putPipelineDefinition(definition);
    log.info("Defining pipeline: " + defineResult);
    logPipelineToDynamo();

    final ActivatePipelineRequest activate = new ActivatePipelineRequest();
    activate.setPipelineId(pipelineId);
    client.activatePipeline(activate);
  }

  private CreatePipelineRequest getCreateRequest() {
    final CreatePipelineRequest createRequest = new CreatePipelineRequest();
    createRequest.setName(runId);
    createRequest.setUniqueId(UUID.randomUUID().toString());
    return createRequest;
  }

  private void logPipelineToDynamo() {
    final PipelineExecutionRecord record = PipelineExecutionRecord.find(runId);
    record.setPipelineName(runId);
    record.setConfigString(config.getPaths());
    record.setPipelineCreated(new Date());
    record.setStatus(PipelineExecutionRecord.Status.Created.toString());
    record.save();
  }

  private Pipeline populatePipeline() throws DataConfigurationException, JsonProcessingException {
    final PipelineFactory factory = new PipelineFactory(config, pipelineId);
    final Pipeline pipeline = new Pipeline(runId, config, pipelineId, factory,
        dataIndex.getSchemaVersion(), runId);
    final EmrStartupPipelineSetup setup = new EmrStartupPipelineSetup(pipeline, factory, runId);
    final Phase1PipelineSetup phase1 = new Phase1PipelineSetup(pipeline, factory, codeManager,
        runId, dataIndex);
    final Phase2PipelineSetup phase2 = new Phase2PipelineSetup(pipeline, factory, codeManager);
    final Phase3PipelineSetup phase3 = new Phase3PipelineSetup(pipeline, factory, codeManager,
        runId, dataIndex);

    PipelineObjectBase previousStep;
    previousStep = setup.populate();
    previousStep = phase1.populate(previousStep);
    previousStep = phase2.populate(previousStep);
    previousStep = phase3.populate(previousStep);
    previousStep.setSuccess(getSuccessAction(factory));
    return pipeline;
  }

  private PipelineObjectBase getSuccessAction(final PipelineFactory factory)
      throws JsonProcessingException {
    final String subject = "PipelineSuccess";
    final PipelineCompletionMessage success = new PipelineCompletionMessage(pipelineId, runId,
        config.getReportBucket(), config.getSuccessSnsArn(), config.getPipelineDynamoTable());
    final String msg = new ObjectMapper().writeValueAsString(success);
    return factory.getSns("PipelineComplete", subject, msg, config.getCompletionSnsArn());
  }

}