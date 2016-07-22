package edu.harvard.data.pipeline;

import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "DummyTableName")
public class PipelineExecutionRecord {
  private static final Logger log = LogManager.getLogger();

  public enum Status { ProvisioningPhase0, Phase0Running, CreatingPipeline, ProvisioningPipeline, PipelineRunning, Success, Failed }

  private static DynamoDBMapper mapper;
  private static DynamoDBMapperConfig mapperConfig;
  private static String tableName;

  public PipelineExecutionRecord(final String runId) {
    this.runId = runId;
  }

  public PipelineExecutionRecord() {
  }

  public static void init(final String table) {
    mapper = new DynamoDBMapper(new AmazonDynamoDBClient());
    tableName = table;
    mapperConfig = new DynamoDBMapperConfig(new TableNameOverride(tableName));
  }

  @DynamoDBHashKey(attributeName = "run_id")
  private String runId;

  @DynamoDBAttribute(attributeName = "run_start")
  private Date runStart;

  @DynamoDBAttribute(attributeName = "phase_0_request_id")
  private String phase0RequestId;

  @DynamoDBAttribute(attributeName = "phase_0_start")
  private Date phase0Start;

  @DynamoDBAttribute(attributeName = "phase_0_success")
  private Boolean phase0Success;

  @DynamoDBAttribute(attributeName = "phase_0_instance_id")
  private String phase0InstanceId;

  @DynamoDBAttribute(attributeName = "phase_0_ip")
  private String phase0Ip;

  @DynamoDBAttribute(attributeName = "pipeline_created")
  private Date pipelineCreated;

  @DynamoDBAttribute(attributeName = "pipeline_id")
  private String pipelineId;

  @DynamoDBAttribute(attributeName = "emr_id")
  private String emrId;

  @DynamoDBAttribute(attributeName = "pipeline_start")
  private Date pipelineStart;

  @DynamoDBAttribute(attributeName = "pipeline_end")
  private Date pipelineEnd;

  @DynamoDBAttribute(attributeName = "bootstrap_log_stream")
  private String bootstrapLogStream;

  @DynamoDBAttribute(attributeName = "bootstrap_log_group")
  private String bootstrapLogGroup;

  @DynamoDBAttribute(attributeName = "cleanup_log_stream")
  private String cleanupLogStream;

  @DynamoDBAttribute(attributeName = "cleanup_log_group")
  private String cleanupLogGroup;

  @DynamoDBAttribute(attributeName = "config")
  private String configString;

  @DynamoDBAttribute(attributeName = "current_step")
  private String currentStep;

  @DynamoDBAttribute(attributeName = "current_step_start")
  private Date currentStepStart;

  @DynamoDBAttribute(attributeName = "previous_steps")
  private String previousSteps;

  @DynamoDBAttribute(attributeName = "status")
  private String status;

  public static PipelineExecutionRecord find(final String runId) {
    if (tableName == null) {
      throw new RuntimeException("PipelineExecutionRecord object saved before init(tableName) method called");
    }
    log.debug("Finding pipeline ID " + runId + " from table " + tableName);
    return mapper.load(PipelineExecutionRecord.class, runId, mapperConfig);
  }

  public void save() {
    if (tableName == null) {
      throw new RuntimeException("PipelineExecutionRecord object saved before init(tableName) method called");
    }
    log.info("Saving pipeline run ID " + runId + " to table " + tableName);
    mapper.save(this, mapperConfig);
  }

  public String getRunId() {
    return runId;
  }

  public void setRunId(final String runId) {
    this.runId = runId;
  }

  public Date getPipelineStart() {
    return pipelineStart;
  }

  public void setPipelineStart(final Date pipelineStart) {
    this.pipelineStart = pipelineStart;
  }

  public String getConfigString() {
    return configString;
  }

  public void setConfigString(final String configString) {
    this.configString = configString;
  }

  public String getCurrentStep() {
    return currentStep;
  }

  public void setCurrentStep(final String currentStep) {
    this.currentStep = currentStep;
  }

  public Date getCurrentStepStart() {
    return currentStepStart;
  }

  public void setCurrentStepStart(final Date currentStepStart) {
    this.currentStepStart = currentStepStart;
  }

  public String getPreviousSteps() {
    return previousSteps;
  }

  public void setPreviousSteps(final String previousSteps) {
    this.previousSteps = previousSteps;
  }

  public Date getPipelineCreated() {
    return pipelineCreated;
  }

  public void setPipelineCreated(final Date pipelineCreated) {
    this.pipelineCreated = pipelineCreated;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(final String status) {
    this.status = status;
  }

  public Date getPipelineEnd() {
    return pipelineEnd;
  }

  public void setPipelineEnd(final Date pipelineEnd) {
    this.pipelineEnd = pipelineEnd;
  }

  public String getBootstrapLogStream() {
    return bootstrapLogStream;
  }

  public void setBootstrapLogStream(final String bootstrapLogStream) {
    this.bootstrapLogStream = bootstrapLogStream;
  }

  public String getBootstrapLogGroup() {
    return bootstrapLogGroup;
  }

  public void setBootstrapLogGroup(final String bootstrapLogGroup) {
    this.bootstrapLogGroup = bootstrapLogGroup;
  }

  public Date getRunStart() {
    return runStart;
  }

  public void setRunStart(final Date runStart) {
    this.runStart = runStart;
  }

  public String getPhase0RequestId() {
    return phase0RequestId;
  }

  public void setPhase0RequestId(final String phase0RequestId) {
    this.phase0RequestId = phase0RequestId;
  }

  public Date getPhase0Start() {
    return phase0Start;
  }

  public void setPhase0Start(final Date phase0Start) {
    this.phase0Start = phase0Start;
  }

  public Boolean getPhase0Success() {
    return phase0Success;
  }

  public void setPhase0Success(final Boolean phase0Success) {
    this.phase0Success = phase0Success;
  }

  public String getCleanupLogStream() {
    return cleanupLogStream;
  }

  public void setCleanupLogStream(final String cleanupLogStream) {
    this.cleanupLogStream = cleanupLogStream;
  }

  public String getCleanupLogGroup() {
    return cleanupLogGroup;
  }

  public void setCleanupLogGroup(final String cleanupLogGroup) {
    this.cleanupLogGroup = cleanupLogGroup;
  }

  public String getPhase0InstanceId() {
    return phase0InstanceId;
  }

  public void setPhase0InstanceId(final String phase0InstanceId) {
    this.phase0InstanceId = phase0InstanceId;
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public void setPipelineId(final String pipelineId) {
    this.pipelineId = pipelineId;
  }

  public String getEmrId() {
    return emrId;
  }

  public void setEmrId(final String emrId) {
    this.emrId = emrId;
  }

  public String getPhase0Ip() {
    return phase0Ip;
  }

  public void setPhase0Ip(String phase0Ip) {
    this.phase0Ip = phase0Ip;
  }

}

class PreviousStepDescription {
  private String step;
  private Date start;
  private Date end;

  public PreviousStepDescription() {}

  public PreviousStepDescription(final String step, final Date start, final Date end) {
    this.step = step;
    this.start = start;
    this.end = end;
  }

  public String getStep() {
    return step;
  }

  public Date getStart() {
    return start;
  }

  public Date getEnd() {
    return end;
  }

}
