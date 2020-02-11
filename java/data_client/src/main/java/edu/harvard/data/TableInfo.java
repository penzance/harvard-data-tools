package edu.harvard.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient; // Deprecated
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
//import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig; // Deprectated
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "DummyTableName")
public class TableInfo {

  private static final Logger log = LogManager.getLogger();

  private static DynamoDBMapper mapper;
  private static String tableName;
  private static DynamoDBMapperConfig.Builder mapperBuilder;
  private static DynamoDBMapperConfig mapperConfig;

  public static void init(final String table) {
    mapper = new DynamoDBMapper(AmazonDynamoDBClientBuilder.defaultClient());
    tableName = table;
    mapperBuilder = new DynamoDBMapperConfig.Builder();
    mapperConfig = mapperBuilder.build();
    mapperBuilder.setTableNameOverride(new TableNameOverride(tableName));
  }

  @DynamoDBHashKey(attributeName = "name")
  private String name;

  @DynamoDBAttribute(attributeName = "last_complete_dump_id")
  private String lastCompleteDumpId;

  @DynamoDBAttribute(attributeName = "last_complete_dump_sequence")
  private long lastCompleteDumpSequence;

  public TableInfo() {
  }

  public TableInfo(final String name) {
    this();
    this.name = name;
  }

  public TableInfo(final String name, final String lastCompleteDumpId,
      final long lastCompleteDumpSequence) {
    this(name);
    this.lastCompleteDumpId = lastCompleteDumpId;
    this.lastCompleteDumpSequence = lastCompleteDumpSequence;
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public String getLastCompleteDumpId() {
    return lastCompleteDumpId;
  }

  public void setLastCompleteDumpId(final String lastCompleteDumpId) {
    this.lastCompleteDumpId = lastCompleteDumpId;
  }

  public long getLastCompleteDumpSequence() {
    return lastCompleteDumpSequence;
  }

  public void setLastCompleteDumpSequence(final long lastCompleteDumpSequence) {
    this.lastCompleteDumpSequence = lastCompleteDumpSequence;
  }

  public static TableInfo find(final String name) {
    if (tableName == null) {
      throw new RuntimeException("TableInfo.find called before init(tableName) method called");
    }
    log.debug("Finding table info for " + name + " from table " + tableName);
    return mapper.load(TableInfo.class, name, mapperConfig);
  }

  public void save() {
    if (tableName == null) {
      throw new RuntimeException("TableInfo object saved before init(tableName) method called");
    }
    log.info("Saving table name " + name + " to table " + tableName);
    mapper.save(this, mapperConfig);
  }

}
