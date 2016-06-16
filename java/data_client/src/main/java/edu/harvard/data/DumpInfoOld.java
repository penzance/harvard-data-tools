package edu.harvard.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.s3.model.S3ObjectId;

@DynamoDBTable(tableName = "DummyTableName")
public class DumpInfoOld {

  private static final Logger log = LogManager.getLogger();

  private static DynamoDBMapper mapper;
  private static String tableName;
  private static DynamoDBMapperConfig mapperConfig;

  public static void init(final String table) {
    mapper = new DynamoDBMapper(new AmazonDynamoDBClient());
    tableName = table;
    mapperConfig = new DynamoDBMapperConfig(new TableNameOverride(tableName));
  }

  @DynamoDBHashKey(attributeName = "id")
  private String id;

  @DynamoDBAttribute(attributeName = "sequence")
  private Long sequence;

  @DynamoDBAttribute(attributeName = "downloaded")
  private boolean downloaded;

  @DynamoDBAttribute(attributeName = "verified")
  private boolean verified;

  @DynamoDBAttribute(attributeName = "s3Bucket")
  private String bucket;

  @DynamoDBAttribute(attributeName = "s3Key")
  private String key;

  @DynamoDBAttribute(attributeName = "schemaVersion")
  private String schemaVersion;

  @DynamoDBAttribute(attributeName = "downloadStart")
  private Date downloadStart;

  @DynamoDBAttribute(attributeName = "downloadEnd")
  private Date downloadEnd;

  public DumpInfoOld() {
    downloaded = false;
    verified = false;
  }

  public DumpInfoOld(final String id) {
    this();
    this.id = id;
  }

  public DumpInfoOld(final String id, final long sequence, final String schemaVersion) {
    this(id);
    this.sequence = sequence;
    this.schemaVersion = schemaVersion;
  }

  public String getId() {
    return id;
  }

  public void setId(final String id) {
    this.id = id;
  }

  public Long getSequence() {
    return sequence;
  }

  public void setSequence(final Long sequence) {
    this.sequence = sequence;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(final String schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public Boolean getDownloaded() {
    return downloaded;
  }

  public void setDownloaded(final Boolean downloaded) {
    this.downloaded = downloaded;
  }

  public Boolean getVerified() {
    return verified;
  }

  public void setVerified(final Boolean verified) {
    this.verified = verified;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(final String bucket) {
    this.bucket = bucket;
  }

  public String getKey() {
    return key;
  }

  public void setKey(final String key) {
    this.key = key;
  }

  public Date getDownloadStart() {
    return downloadStart;
  }

  public void setDownloadStart(final Date downloadStart) {
    this.downloadStart = downloadStart;
  }

  public Date getDownloadEnd() {
    return downloadEnd;
  }

  public void setDownloadEnd(final Date downloadEnd) {
    this.downloadEnd = downloadEnd;
  }

  public static DumpInfoOld find(final String dumpId) {
    if (tableName == null) {
      throw new RuntimeException("DumpInfo.find called before init(tableName) method");
    }
    log.debug("Finding dump ID " + dumpId + " from table " + tableName);
    return mapper.load(DumpInfoOld.class, dumpId, mapperConfig);
  }

  public static List<DumpInfoOld> getAllDumpsSince(final long startSequence) {
    if (tableName == null) {
      throw new RuntimeException("DumpInfo.getAllDumpsSince called before init(tableName) method");
    }
    final DynamoDBScanExpression scan = new DynamoDBScanExpression();
    scan.setFilterExpression("#sequence >= :start");
    scan.setExpressionAttributeNames(Collections.singletonMap("#sequence", "sequence"));
    scan.setExpressionAttributeValues(
        Collections.singletonMap(":start", new AttributeValue().withN("" + startSequence)));
    log.debug("Finding all dumps since " + startSequence + " from table " + tableName);
    final PaginatedScanList<DumpInfoOld> results = mapper.scan(DumpInfoOld.class, scan, mapperConfig);
    results.loadAllResults();

    final List<DumpInfoOld> dumps = new ArrayList<DumpInfoOld>();
    for (final DumpInfoOld result : results) {
      dumps.add(result);
    }
    return dumps;
  }

  @DynamoDBIgnore
  public S3ObjectId getS3Location() {
    return AwsUtils.key(bucket, key);
  }

  public void save() {
    if (tableName == null) {
      throw new RuntimeException("DumpInfo object saved before init(tableName) method called");
    }
    log.info("Saving dump info sequence " + sequence + " to table " + tableName);
    mapper.save(this, mapperConfig);
  }

  public void resetDownloadAndVerify() {
    this.downloaded = false;
    this.downloadEnd = null;
    this.downloadStart = null;
    this.verified = false;
    this.bucket = null;
    this.save();
  }
}
