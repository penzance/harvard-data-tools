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

  private static DynamoDBMapper mapper;
  private static DynamoDBMapperConfig mapperConfig;
  private static String tableName;

  public PipelineExecutionRecord(final String pipelineId) {
    this.pipelineId = pipelineId;
  }

  public PipelineExecutionRecord() {
  }

  public static void init(final String table) {
    mapper = new DynamoDBMapper(new AmazonDynamoDBClient());
    tableName = table;
    mapperConfig = new DynamoDBMapperConfig(new TableNameOverride(tableName));
  }

  @DynamoDBHashKey(attributeName = "pipeline_id")
  private String pipelineId;

  @DynamoDBAttribute(attributeName = "pipeline_name")
  private String pipelineName;

  @DynamoDBAttribute(attributeName = "pipeline_created")
  private Date pipelineCreated;

  @DynamoDBAttribute(attributeName = "pipeline_start")
  private Date pipelineStart;

  @DynamoDBAttribute(attributeName = "config")
  private String configString;

  @DynamoDBAttribute(attributeName = "current_step")
  private String currentStep;

  @DynamoDBAttribute(attributeName = "current_step_start")
  private Date currentStepStart;

  @DynamoDBAttribute(attributeName = "previousSteps")
  private String previousSteps;

  public static PipelineExecutionRecord find(final String pipelineId) {
    if (tableName == null) {
      throw new RuntimeException("PipelineExecutionRecord object saved before init(tableName) method called");
    }
    log.debug("Finding pipeline ID " + pipelineId + " from table " + tableName);
    return mapper.load(PipelineExecutionRecord.class, pipelineId, mapperConfig);
  }

  public void save() {
    if (tableName == null) {
      throw new RuntimeException("PipelineExecutionRecord object saved before init(tableName) method called");
    }
    log.info("Saving pipeline record ID " + pipelineId + " to table " + tableName);
    mapper.save(this, mapperConfig);
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public void setPipelineId(final String pipelineId) {
    this.pipelineId = pipelineId;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public void setPipelineName(final String pipelineName) {
    this.pipelineName = pipelineName;
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
