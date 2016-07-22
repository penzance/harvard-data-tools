package edu.harvard.data;

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
public class DatasetInfo {
  private static final Logger log = LogManager.getLogger();

  private static DynamoDBMapper mapper;
  private static DynamoDBMapperConfig mapperConfig;
  private static String tableName;

  public DatasetInfo(final String name) {
    this.setName(name);
  }

  public DatasetInfo() {
  }

  public static void init(final String table) {
    mapper = new DynamoDBMapper(new AmazonDynamoDBClient());
    tableName = table;
    mapperConfig = new DynamoDBMapperConfig(new TableNameOverride(tableName));
  }

  @DynamoDBHashKey(attributeName = "name")
  private String name;

  @DynamoDBAttribute(attributeName = "run_id")
  private String runId;

  public static DatasetInfo find(final String name) {
    if (tableName == null) {
      throw new RuntimeException("DatasetInfo object saved before init(tableName) method called");
    }
    log.debug("Finding dataset info " + name + " from table " + tableName);
    return mapper.load(DatasetInfo.class, name, mapperConfig);
  }

  public void save() {
    if (tableName == null) {
      throw new RuntimeException("DatasetInfo object saved before init(tableName) method called");
    }
    log.info("Saving dataset name " + getName() + " to table " + tableName);
    mapper.save(this, mapperConfig);
  }

  public String getRunId() {
    return runId;
  }

  public void setRunId(final String runId) {
    this.runId = runId;
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

}