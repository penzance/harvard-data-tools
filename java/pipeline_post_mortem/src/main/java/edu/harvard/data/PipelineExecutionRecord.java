package edu.harvard.data;

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

// TODO: This is a direct copy/paste from data-tools, and needs to be fixed.
// The problem is that Lambda has a maximum Jar size of 50mb (250mb uncompressed), and
// the complete data tools jar is already pushing that size. Introducing a dependency there
// will push the size of the post mortem Jar very close to the limit, and it will probably
// sneak over it at some point later causing a hard-to-debug failure at some later date.
// Still - there's got to be a better solution than this.
@DynamoDBTable(tableName = "DummyTableName")
public class PipelineExecutionRecord {
  private static final Logger log = LogManager.getLogger();

  public enum Status { Starting, Running, Failed, Succcess }

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

  @DynamoDBAttribute(attributeName = "pipeline_created")
  private Date pipelineCreated;

  @DynamoDBAttribute(attributeName = "pipeline_name")
  private String pipelineName;

  @DynamoDBAttribute(attributeName = "pipeline_start")
  private Date pipelineStart;

  @DynamoDBAttribute(attributeName = "pipeline_end")
  private Date pipelineEnd;

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
