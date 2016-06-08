package edu.harvard.data.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

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
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.generator.GeneratedCodeManager;
import edu.harvard.data.generator.GenerationSpec;

public class DataPipelineGenerator {
  private static final Logger log = LogManager.getLogger();

  private final GenerationSpec spec;
  private final DataConfig config;
  private final String name;
  private String pipelineId;
  private final S3ObjectId dataLocation;
  private final GeneratedCodeManager codeManager;

  public DataPipelineGenerator(final String name, final GenerationSpec spec,
      final DataConfig config, final S3ObjectId dataLocation, final GeneratedCodeManager codeManager) {
    this.name = name;
    this.spec = spec;
    this.config = config;
    this.dataLocation = dataLocation;
    this.codeManager = codeManager;
  }

  public void generate() throws DataConfigurationException, IOException {
    log.info("Data at " + AwsUtils.uri(dataLocation));
    PipelineExecutionRecord.init(config.pipelineDynamoTable);
    final DataPipelineClient client = new DataPipelineClient();
    final CreatePipelineRequest create = getCreateRequest();
    final CreatePipelineResult createResult = client.createPipeline(create);
    pipelineId = createResult.getPipelineId();
    log.info(createResult);

    final Pipeline pipeline = populatePipeline();

    final PutPipelineDefinitionRequest definition = pipeline.getDefineRequest(pipelineId);
    final PutPipelineDefinitionResult defineResult = client.putPipelineDefinition(definition);
    logPipelineToDynamo();
    log.info(defineResult);
  }

  private CreatePipelineRequest getCreateRequest() {
    final CreatePipelineRequest createRequest = new CreatePipelineRequest();
    createRequest.setName(name);
    createRequest.setUniqueId(UUID.randomUUID().toString());
    return createRequest;
  }

  private void logPipelineToDynamo() {
    final PipelineExecutionRecord record = new PipelineExecutionRecord(pipelineId);
    record.setPipelineName(name);
    record.setConfigString(config.paths);
    record.setPipelineCreated(new Date());
    record.setStatus(PipelineExecutionRecord.Status.Created.toString());
    record.save();
  }

  private Pipeline populatePipeline() throws DataConfigurationException, JsonProcessingException {
    final PipelineFactory factory = new PipelineFactory(config, pipelineId);
    final Pipeline pipeline = new Pipeline(name, spec, config, pipelineId, factory);
    final EmrStartupPipelineSetup setup = new EmrStartupPipelineSetup(pipeline, factory,
        dataLocation);
    final Phase1PipelineSetup phase1 = new Phase1PipelineSetup(pipeline, factory, codeManager);
    final List<Phase2PipelineSetup> phase2 = new ArrayList<Phase2PipelineSetup>();
    for (int i = 0; i < 2; i++) {
      phase2.add(new Phase2PipelineSetup(pipeline, factory, i));
    }
    final Phase3PipelineSetup phase3 = new Phase3PipelineSetup(pipeline, factory);

    PipelineObjectBase previousStep;
    previousStep = setup.populate();
    previousStep = phase1.populate(previousStep);
    // for (final Phase2PipelineSetup subphase : phase2) {
    // previousStep = subphase.populate(previousStep);
    // }
    // previousStep = phase3.populate(previousStep);
    previousStep.setSuccess(getSuccessAction(factory));
    return pipeline;
  }

  private PipelineObjectBase getSuccessAction(final PipelineFactory factory)
      throws JsonProcessingException {
    final String subject = "PipelineSuccess";
    final PipelineCompletionMessage success = new PipelineCompletionMessage(pipelineId,
        config.reportBucket, config.successSnsArn, config.pipelineDynamoTable);
    final String msg = new ObjectMapper().writeValueAsString(success);
    return factory.getSns("PipelineComplete", subject, msg, config.completionSnsArn);
  }

}