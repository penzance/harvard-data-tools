package edu.harvard.data.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionRequest;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.fasterxml.jackson.core.JsonProcessingException;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;

public class Pipeline {

  private final DataConfig config;
  private final String pipelineId;
  private final String name;
  private final PipelineFactory factory;
  private final PipelineObjectBase redshift;
  private final PipelineObjectBase emr;
  private final PipelineObjectBase schedule;
  private final String schemaVersion;
  private final String runId;
  
  private static final Logger log = LogManager.getLogger();


  public Pipeline(final String name, final DataConfig config, final String pipelineId,
      final PipelineFactory factory, final String schemaVersion, final String runId)
          throws JsonProcessingException {
    this.name = name;
    this.config = config;
    this.pipelineId = pipelineId;
    this.factory = factory;
    this.schemaVersion = schemaVersion;
    this.runId = runId;
    this.schedule = factory.getSchedule();
    this.redshift = factory.getRedshift();
    this.emr = createEmr();
    createDefaultObject();
  }

  public PutPipelineDefinitionRequest getDefineRequest(final String pipelineId)
      throws DataConfigurationException {
    final PutPipelineDefinitionRequest defineRequest = new PutPipelineDefinitionRequest();
    defineRequest.setPipelineId(pipelineId);
    defineRequest.setPipelineObjects(factory.getAllObjects());
    return defineRequest;
  }

  private PipelineObjectBase createDefaultObject() throws JsonProcessingException {
    final PipelineObjectBase defaultObj = factory.getDefault(schedule);
    return defaultObj;
  }

  private PipelineObjectBase createEmr() {
    final PipelineObjectBase emr = factory.getEmr(name + "_Emr_Cluster");
    log.info("Pipelineobject: " + emr.getPipelineObject());
    emr.set("bootstrapAction", emrBootstrapAction());
    return emr;
  }

  private String emrBootstrapAction() {
    final S3ObjectId script = AwsUtils.key(config.getCodeBucket(), config.getGitTagOrBranch(),
        "emr_bootstrap.sh");
    final List<String> bootstrapParams = new ArrayList<String>();
    bootstrapParams.add(AwsUtils.uri(script));
    bootstrapParams.add(schemaVersion);
    bootstrapParams.add(config.getGitTagOrBranch());
    bootstrapParams.add(config.getDataSource().toLowerCase() + "_generate_tools.py");
    bootstrapParams.add(config.getPaths());
    bootstrapParams.add(runId);
    bootstrapParams.add(config.getEmrCodeDir());
    return StringUtils.join(bootstrapParams, ",");
  }

  public DataConfig getConfig() {
    return config;
  }

  public String getId() {
    return pipelineId;
  }

  public PipelineObjectBase getEmr() {
    return emr;
  }

  public PipelineObjectBase getRedshift() {
    return redshift;
  }

}
